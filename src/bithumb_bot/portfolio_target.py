from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from .canonical_decision import sha256_prefixed
from .risk_decision import (
    RISK_BUDGET_LEGACY_MARKER,
    RISK_BUDGET_SEMANTICS,
    build_risk_decision_artifact,
)


@dataclass(frozen=True)
class PortfolioTarget:
    pair: str
    target_exposure_krw: float | None
    target_qty: float | None
    allocator_policy_name: str
    allocator_policy_version: str
    allocator_config_hash: str
    strategy_contribution_hash: str
    allocation_input_hash: str
    reason: str
    conflict_resolution: Mapping[str, object] = field(default_factory=dict)
    authoritative: bool = True
    fail_closed_reason: str = "none"
    schema_version: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "target_exposure_krw",
            None if self.target_exposure_krw is None else float(self.target_exposure_krw),
        )
        object.__setattr__(
            self,
            "target_qty",
            None if self.target_qty is None else float(self.target_qty),
        )
        object.__setattr__(
            self,
            "conflict_resolution",
            {str(key): value for key, value in dict(self.conflict_resolution).items()},
        )

    def _payload(self) -> dict[str, object]:
        risk_decision = build_risk_decision_artifact(
            max_target_exposure_krw=self.conflict_resolution.get("exposure_cap_krw"),
            exposure_cap_source=str(self.conflict_resolution.get("exposure_cap_source", "none")),
            decision_context="portfolio_target",
        )
        return {
            "schema_version": int(self.schema_version),
            "pair": self.pair,
            "target_exposure_krw": self.target_exposure_krw,
            "max_target_exposure_krw": self.target_exposure_krw,
            "pre_cap_weighted_target_exposure_krw": self.conflict_resolution.get(
                "pre_cap_weighted_target_exposure_krw"
            ),
            "exposure_cap_krw": self.conflict_resolution.get("exposure_cap_krw"),
            "exposure_cap_applied": bool(self.conflict_resolution.get("exposure_cap_applied", False)),
            "exposure_cap_source": self.conflict_resolution.get("exposure_cap_source", "none"),
            "target_qty": self.target_qty,
            "allocator_policy_name": self.allocator_policy_name,
            "allocator_policy_version": self.allocator_policy_version,
            "allocator_config_hash": self.allocator_config_hash,
            "strategy_contribution_hash": self.strategy_contribution_hash,
            "allocation_input_hash": self.allocation_input_hash,
            "reason": self.reason,
            "conflict_resolution": dict(self.conflict_resolution),
            "authoritative": bool(self.authoritative),
            "fail_closed_reason": self.fail_closed_reason,
            "risk_budget_semantics": RISK_BUDGET_SEMANTICS,
            "risk_decision": risk_decision,
            "risk_decision_hash": risk_decision["risk_decision_hash"],
            "risk_budget_legacy_marker": RISK_BUDGET_LEGACY_MARKER,
        }

    def content_hash(self) -> str:
        return sha256_prefixed(self._payload())

    def as_dict(self) -> dict[str, object]:
        payload = self._payload()
        payload["final_portfolio_target_hash"] = self.content_hash()
        return payload
