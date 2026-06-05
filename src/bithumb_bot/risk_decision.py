from __future__ import annotations

from typing import Mapping

from .canonical_decision import sha256_prefixed


RISK_BUDGET_SEMANTICS = "deprecated_non_authoritative_not_exposure_cap"
RISK_BUDGET_LEGACY_MARKER = "deprecated:risk_budget_krw_not_enforced_as_loss_budget"
RISK_DECISION_AUTHORITY_LABEL = "RiskDecision.exposure_cap_boundary.v1"


def build_risk_decision_artifact(
    *,
    risk_budget_krw: object | None = None,
    max_target_exposure_krw: object | None = None,
    exposure_cap_source: str = "none",
    decision_context: str = "runtime_submit_authority",
) -> dict[str, object]:
    """Return the current risk/exposure boundary artifact.

    This deliberately does not claim a loss-budget risk engine. It records the
    fail-closed semantic boundary: `risk_budget_krw` is not exposure authority
    and `max_target_exposure_krw` is the only exposure-cap field.
    """

    payload = {
        "schema_version": 1,
        "authority_label": RISK_DECISION_AUTHORITY_LABEL,
        "decision_context": str(decision_context or "runtime_submit_authority"),
        "risk_budget_krw": risk_budget_krw,
        "risk_budget_semantics": RISK_BUDGET_SEMANTICS,
        "risk_budget_interpreted_as_exposure_cap": False,
        "loss_budget_supported": False,
        "loss_budget_authority": "unsupported_fail_closed",
        "max_target_exposure_krw": max_target_exposure_krw,
        "exposure_cap_source": str(exposure_cap_source or "none"),
        "risk_budget_legacy_marker": RISK_BUDGET_LEGACY_MARKER,
    }
    payload["risk_decision_hash"] = risk_decision_hash(payload)
    return payload


def risk_decision_hash(payload: Mapping[str, object]) -> str:
    body = {str(key): value for key, value in dict(payload).items() if key != "risk_decision_hash"}
    return sha256_prefixed(body)
