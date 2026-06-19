from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .h74_observation import H74_OBSERVATION_AUTHORITY_ARTIFACT_TYPE
from .operator_smoke_authority import OPERATOR_SMOKE_AUTHORITY_ARTIFACT_TYPE


APPROVED_PROFILE_AUTHORITY_TYPE = "approved_profile_authority"
LIVE_OBSERVATION_AUTHORITY_TYPE = "live_observation_authority"
OPERATOR_SMOKE_AUTHORITY_TYPE = "operator_smoke_authority"
EMERGENCY_CLOSEOUT_AUTHORITY_TYPE = "emergency_closeout_authority"


@dataclass(frozen=True)
class ExecutionAuthority:
    authority_type: str
    allowed_operations: tuple[str, ...]
    market_scope: tuple[str, ...]
    notional_cap: float | None
    expires_at: str | None
    parameter_authority: bool
    exit_policy_authority: bool
    risk_authority: bool
    evidence_classification: str
    identity_hash: str

    def allows(self, operation: str) -> bool:
        return str(operation or "") in set(self.allowed_operations)


def _identity_hash(payload: Mapping[str, Any]) -> str:
    for key in ("authority_content_hash", "profile_content_hash", "identity_hash"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""


def execution_authority_from_payload(payload: Mapping[str, Any]) -> ExecutionAuthority:
    artifact_type = str(payload.get("artifact_type") or payload.get("authority_type") or "").strip()
    if artifact_type == OPERATOR_SMOKE_AUTHORITY_ARTIFACT_TYPE:
        return ExecutionAuthority(
            authority_type=OPERATOR_SMOKE_AUTHORITY_TYPE,
            allowed_operations=("operator_smoke_buy", "operator_smoke_sell"),
            market_scope=(str(payload.get("market") or "").strip().upper() or "*",),
            notional_cap=float(payload.get("max_notional_krw") or 0.0),
            expires_at=str(payload.get("expires_at") or "") or None,
            parameter_authority=False,
            exit_policy_authority=False,
            risk_authority=False,
            evidence_classification="operator_smoke_only",
            identity_hash=_identity_hash(payload),
        )
    if artifact_type == H74_OBSERVATION_AUTHORITY_ARTIFACT_TYPE:
        bound = payload.get("hash_bound_parameters") if isinstance(payload.get("hash_bound_parameters"), Mapping) else {}
        risk_authority = bool(payload.get("risk_authority")) and bool(payload.get("risk_policy_hash"))
        return ExecutionAuthority(
            authority_type=LIVE_OBSERVATION_AUTHORITY_TYPE,
            allowed_operations=("h74_live_observation_50k",),
            market_scope=(str(bound.get("market") or "KRW-BTC").strip().upper(),),
            notional_cap=float(bound.get("max_notional_krw") or 0.0),
            expires_at=str(bound.get("expires_at") or "") or None,
            parameter_authority=bool(bound),
            exit_policy_authority=bool(payload.get("exit_policy_authority")) and bool(payload.get("exit_policy_hash")),
            risk_authority=risk_authority,
            evidence_classification="live_observation_non_substitutive",
            identity_hash=_identity_hash(payload),
        )
    if artifact_type in {"approved_profile", APPROVED_PROFILE_AUTHORITY_TYPE} or payload.get("profile_content_hash"):
        market = str(payload.get("market") or payload.get("pair") or "*").strip().upper()
        return ExecutionAuthority(
            authority_type=APPROVED_PROFILE_AUTHORITY_TYPE,
            allowed_operations=("strategy_run", "strategy_live_dry_run", "small_live"),
            market_scope=(market,),
            notional_cap=None,
            expires_at=None,
            parameter_authority=True,
            exit_policy_authority=True,
            risk_authority=True,
            evidence_classification="approved_profile",
            identity_hash=_identity_hash(payload),
        )
    if artifact_type == EMERGENCY_CLOSEOUT_AUTHORITY_TYPE:
        return ExecutionAuthority(
            authority_type=EMERGENCY_CLOSEOUT_AUTHORITY_TYPE,
            allowed_operations=("position_reduction", "cancel_open_orders"),
            market_scope=(str(payload.get("market") or "*").strip().upper(),),
            notional_cap=float(payload.get("notional_cap") or 0.0) if payload.get("notional_cap") is not None else None,
            expires_at=str(payload.get("expires_at") or "") or None,
            parameter_authority=False,
            exit_policy_authority=False,
            risk_authority=True,
            evidence_classification="emergency_closeout",
            identity_hash=_identity_hash(payload),
        )
    raise ValueError(f"unknown_execution_authority_type:{artifact_type or 'missing'}")


def resolve_execution_authority(
    command_intent: str,
    settings: object,
    args_or_payload: Mapping[str, Any] | str | Path | object,
) -> ExecutionAuthority:
    del settings
    payload: Mapping[str, Any] | None = None
    if isinstance(args_or_payload, Mapping):
        payload = args_or_payload
    elif isinstance(args_or_payload, (str, Path)):
        with Path(args_or_payload).expanduser().open("r", encoding="utf-8") as handle:
            decoded = json.load(handle)
        if not isinstance(decoded, Mapping):
            raise ValueError("execution_authority_payload_not_object")
        payload = decoded
    else:
        candidate = getattr(args_or_payload, "payload", None)
        if isinstance(candidate, Mapping):
            payload = candidate
    if payload is None:
        raise ValueError("execution_authority_payload_missing")
    authority = execution_authority_from_payload(payload)
    intent = str(command_intent or "").strip()
    operation = {
        "smoke-buy": "operator_smoke_buy",
        "operator_smoke_buy": "operator_smoke_buy",
        "h74-observation": "h74_live_observation_50k",
        "h74_live_observation_50k": "h74_live_observation_50k",
        "strategy-run": "strategy_run",
        "strategy_run": "strategy_run",
    }.get(intent, intent)
    if operation:
        require_authority_operation(authority, operation)
    return authority


def require_authority_operation(authority: ExecutionAuthority, operation: str) -> None:
    if not authority.allows(operation):
        raise ValueError(
            "execution_authority_operation_not_allowed:"
            f"authority_type={authority.authority_type}:operation={operation}"
        )


def validate_live_observation_authority_complete_for_runtime(authority: ExecutionAuthority) -> None:
    if authority.authority_type != LIVE_OBSERVATION_AUTHORITY_TYPE:
        raise ValueError("live_observation_authority_required")
    if not (authority.parameter_authority and authority.exit_policy_authority and authority.risk_authority):
        raise ValueError("live_observation_authority_requires_parameter_exit_and_risk_authority")
    if authority.expires_at:
        expires_at = datetime.fromisoformat(authority.expires_at.replace("Z", "+00:00"))
        if expires_at <= datetime.now(timezone.utc):
            raise ValueError("live_observation_authority_expired")
