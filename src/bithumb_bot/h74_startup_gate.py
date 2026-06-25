from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .decision_equivalence import sha256_prefixed


@dataclass(frozen=True)
class H74StartupGateResult:
    status: str
    reason_code: str
    recommended_command: str
    details: Mapping[str, Any]

    @property
    def allowed(self) -> bool:
        return self.status in {"START_ALLOWED", "START_ALLOWED_WITH_TERMINAL_DUST"}

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "gate": "h74_startup_precondition",
            "status": self.status,
            "reason_code": self.reason_code,
            "recommended_command": self.recommended_command,
            "details": dict(self.details),
        }
        payload["startup_gate_hash"] = sha256_prefixed(payload)
        return payload


def evaluate_h74_startup_gate(
    *,
    readiness_payload: Mapping[str, Any],
    target_state: Mapping[str, Any] | None = None,
    authority: Mapping[str, Any] | None = None,
) -> H74StartupGateResult:
    payload = dict(readiness_payload or {})
    authority_payload = dict(authority or {})
    broker = dict(payload.get("broker_position_evidence") or {})
    projection = dict(payload.get("projection_convergence") or {})
    residual_state = str(payload.get("residual_inventory_state") or "").strip()
    residual_mode = str(
        authority_payload.get("residual_inventory_mode")
        or payload.get("residual_inventory_mode")
        or "block_executable_residual"
    )
    broker_qty_known = bool(broker.get("broker_qty_known") or "broker_qty" in broker)
    broker_qty = _float(broker.get("broker_qty"))
    portfolio_qty = _float(projection.get("portfolio_qty", payload.get("portfolio_qty")))
    projected_qty = _float(projection.get("projected_total_qty", payload.get("projected_total_qty")))
    target = dict(target_state or {})
    target_live_submit_authority = target.get("live_submit_authority")
    target_is_authoritative = target_live_submit_authority is not False

    checks = {
        "broker_qty_known": broker_qty_known,
        "broker_qty": broker_qty,
        "portfolio_qty": portfolio_qty,
        "projected_total_qty": projected_qty,
        "open_order_count": _int(payload.get("open_order_count", payload.get("unresolved_open_order_count"))),
        "submit_unknown_count": _int(payload.get("submit_unknown_count")),
        "recovery_required_count": _int(payload.get("recovery_required_count")),
        "target_exposure_krw": (
            _float(target.get("target_exposure_krw", payload.get("target_exposure_krw")))
            if target_is_authoritative
            else 0.0
        ),
        "target_live_submit_authority": target_live_submit_authority,
        "residual_inventory_state": residual_state,
        "residual_inventory_mode": residual_mode,
    }

    def block(reason: str) -> H74StartupGateResult:
        return H74StartupGateResult(
            status="START_BLOCKED",
            reason_code=reason,
            recommended_command="run h74 readiness/recovery preflight before starting live h74",
            details=checks,
        )

    if not broker_qty_known or broker_qty is None:
        return block("broker_qty_unknown")
    if checks["open_order_count"] > 0:
        return block("open_order_count_nonzero")
    if checks["submit_unknown_count"] > 0:
        return block("submit_unknown_count_nonzero")
    if checks["recovery_required_count"] > 0:
        return block("recovery_required_count_nonzero")
    if portfolio_qty is not None and abs(float(portfolio_qty) - float(broker_qty)) > 1e-12:
        return block("broker_local_qty_mismatch")
    if projected_qty is not None and abs(float(projected_qty) - float(broker_qty)) > 1e-12:
        return block("accounting_projection_qty_mismatch")
    if checks["target_exposure_krw"] is not None and abs(float(checks["target_exposure_krw"])) > 1e-9:
        return block("target_state_nonzero")
    if broker_qty >= 0.0001:
        return block("broker_executable_residual_exists")
    true_dust_allowed = residual_mode == "allow_terminal_true_dust" and residual_state in {
        "true_dust",
        "terminal_true_dust",
        "dust_only",
    }
    if broker_qty > 1e-12 and not true_dust_allowed:
        return block("terminal_dust_policy_missing")
    status = "START_ALLOWED_WITH_TERMINAL_DUST" if broker_qty > 1e-12 else "START_ALLOWED"
    return H74StartupGateResult(
        status=status,
        reason_code="clean_flat_start" if status == "START_ALLOWED" else "terminal_true_dust_allowed",
        recommended_command="none",
        details=checks,
    )


def _float(value: object) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _int(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


__all__ = ["H74StartupGateResult", "evaluate_h74_startup_gate"]
