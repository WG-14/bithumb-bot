from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .decision_equivalence import sha256_prefixed


ENTRY_AUTHORITY_GATE = "entry_authority"
ENTRY_AUTHORITY_ALLOW = "ALLOW"
ENTRY_AUTHORITY_BLOCK = "BLOCK"
ENTRY_AUTHORITY_REASON_FINAL_SIGNAL_BUY = "strategy_final_signal_buy"
ENTRY_AUTHORITY_REASON_DAILY_PARTICIPATION = "daily_participation_entry"
ENTRY_AUTHORITY_REASON_OPERATOR_OR_RECOVERY = "explicit_operator_or_recovery_buy_authority"
ENTRY_AUTHORITY_REASON_EXISTING_TARGET_REBALANCE = "existing_target_rebalance"
ENTRY_AUTHORITY_REASON_NOT_REQUIRED = "not_new_buy_exposure"
ENTRY_AUTHORITY_REASON_BLOCKED = "target_delta_entry_without_strategy_buy_authority"

DAILY_PARTICIPATION_ALLOW_REASONS = frozenset({"daily_participation_fallback_allowed"})
EXPLICIT_BUY_AUTHORITIES = frozenset(
    {
        "manual_operator_entry",
        "operator_buy",
        "operator_entry",
        "recovery_buy",
        "explicit_operator_buy_authority",
        "explicit_recovery_buy_authority",
    }
)


@dataclass(frozen=True)
class EntryAuthorityDecision:
    status: str
    reason_code: str
    source: str
    blocking: bool
    input_hash: str
    evidence_hash: str
    state_source: str = "entry_authority_policy"

    @property
    def allowed(self) -> bool:
        return self.status == ENTRY_AUTHORITY_ALLOW

    def as_dict(self) -> dict[str, object]:
        return {
            "gate": ENTRY_AUTHORITY_GATE,
            "status": self.status,
            "reason_code": self.reason_code,
            "source": self.source,
            "input_hash": self.input_hash,
            "evidence_hash": self.evidence_hash,
            "state_source": self.state_source,
            "blocking": bool(self.blocking),
        }


def _first_text(payload: Mapping[str, object], *keys: str) -> str:
    for key in keys:
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""


def evaluate_entry_authority(
    *,
    payload: Mapping[str, object],
    side: str,
    current_exposure_krw: float | None,
    target_exposure_krw: float | None,
    delta_krw: float | None,
) -> EntryAuthorityDecision:
    normalized_side = str(side or "").strip().upper()
    current_exposure = max(0.0, float(current_exposure_krw or 0.0))
    target_exposure = max(0.0, float(target_exposure_krw or 0.0))
    delta = float(delta_krw or (target_exposure - current_exposure))
    new_buy_exposure = normalized_side == "BUY" and delta > 1e-9 and target_exposure > current_exposure + 1e-9

    final_signal = _first_text(payload, "final_signal", "signal").upper() or "HOLD"
    try:
        previous_target_exposure = max(0.0, float(payload.get("previous_target_exposure_krw") or 0.0))
    except (TypeError, ValueError):
        previous_target_exposure = 0.0
    daily_reason = _first_text(payload, "daily_participation_reason_code", "final_reason", "reason")
    explicit_authority = _first_text(payload, "entry_authority_source", "authority_source", "buy_authority_source")
    existing_target_rebalance = (
        new_buy_exposure
        and previous_target_exposure > 1e-9
        and current_exposure > 1e-9
        and target_exposure <= previous_target_exposure + 1e-9
    )
    input_payload = {
        "side": normalized_side,
        "current_exposure_krw": current_exposure,
        "target_exposure_krw": target_exposure,
        "previous_target_exposure_krw": previous_target_exposure,
        "delta_krw": delta,
        "final_signal": final_signal,
        "daily_participation_reason_code": daily_reason,
        "explicit_authority": explicit_authority,
        "existing_target_rebalance": existing_target_rebalance,
    }
    input_hash = sha256_prefixed(input_payload)

    if not new_buy_exposure:
        reason_code = ENTRY_AUTHORITY_REASON_NOT_REQUIRED
        status = ENTRY_AUTHORITY_ALLOW
        source = "position_management_or_no_new_buy"
    elif final_signal == "BUY":
        reason_code = ENTRY_AUTHORITY_REASON_FINAL_SIGNAL_BUY
        status = ENTRY_AUTHORITY_ALLOW
        source = "strategy_final_signal"
    elif existing_target_rebalance:
        reason_code = ENTRY_AUTHORITY_REASON_EXISTING_TARGET_REBALANCE
        status = ENTRY_AUTHORITY_ALLOW
        source = "existing_target_rebalance"
    elif daily_reason in DAILY_PARTICIPATION_ALLOW_REASONS:
        reason_code = ENTRY_AUTHORITY_REASON_DAILY_PARTICIPATION
        status = ENTRY_AUTHORITY_ALLOW
        source = "daily_participation_fallback_allowed"
    elif explicit_authority in EXPLICIT_BUY_AUTHORITIES:
        reason_code = ENTRY_AUTHORITY_REASON_OPERATOR_OR_RECOVERY
        status = ENTRY_AUTHORITY_ALLOW
        source = explicit_authority
    else:
        reason_code = ENTRY_AUTHORITY_REASON_BLOCKED
        status = ENTRY_AUTHORITY_BLOCK
        source = "none"

    evidence = {
        **input_payload,
        "new_buy_exposure": new_buy_exposure,
        "status": status,
        "reason_code": reason_code,
        "source": source,
    }
    return EntryAuthorityDecision(
        status=status,
        reason_code=reason_code,
        source=source,
        blocking=status == ENTRY_AUTHORITY_BLOCK,
        input_hash=input_hash,
        evidence_hash=sha256_prefixed(evidence),
    )
