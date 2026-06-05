from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Mapping


TARGET_DELTA_SUBMIT_SOURCE = "target_delta"
TARGET_DELTA_SUBMIT_AUTHORITIES = frozenset(
    {
        "canonical_target_delta_sizing",
        "target_position_delta",
    }
)
RESIDUAL_SUBMIT_SOURCE = "residual_inventory"
RESIDUAL_SUBMIT_AUTHORITIES = frozenset({"residual_inventory_policy"})
LEGACY_BUY_SUBMIT_SOURCES = frozenset({"strategy_position"})
LEGACY_BUY_SUBMIT_AUTHORITIES = frozenset(
    {
        "configured_strategy_order_size",
        "residual_inventory_delta",
        "strategy_execution_intent",
        "research_compatibility_execution_intent",
    }
)


@dataclass(frozen=True)
class SubmitAuthorityPolicy:
    submit_authority_mode: str
    live_real_order_requires_target_delta: bool
    legacy_lot_native_compat_enabled: bool
    allowed_submit_plan_sources: tuple[str, ...]
    allowed_submit_plan_authorities: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "submit_authority_mode": self.submit_authority_mode,
            "live_real_order_requires_target_delta": bool(
                self.live_real_order_requires_target_delta
            ),
            "legacy_lot_native_compat_enabled": bool(self.legacy_lot_native_compat_enabled),
            "allowed_submit_plan_sources": list(self.allowed_submit_plan_sources),
            "allowed_submit_plan_authorities": list(self.allowed_submit_plan_authorities),
        }

    def content_hash(self) -> str:
        return submit_authority_policy_hash(self.as_dict())


@dataclass(frozen=True)
class SubmitAuthorityPolicyDecision:
    allowed: bool
    reason: str
    policy: SubmitAuthorityPolicy
    plan_kind: str
    mode: str
    live_dry_run: bool
    live_real_order_armed: bool
    execution_engine: str
    source: str
    authority: str
    side: str
    submit_expected: bool
    pre_submit_proof_status: str

    def as_dict(self) -> dict[str, object]:
        return {
            "allowed": bool(self.allowed),
            "reason": self.reason,
            "plan_kind": self.plan_kind,
            "mode": self.mode,
            "live_dry_run": bool(self.live_dry_run),
            "live_real_order_armed": bool(self.live_real_order_armed),
            "execution_engine": self.execution_engine,
            "source": self.source,
            "authority": self.authority,
            "side": self.side,
            "submit_expected": bool(self.submit_expected),
            "pre_submit_proof_status": self.pre_submit_proof_status,
            "submit_authority_mode": self.policy.submit_authority_mode,
            "submit_authority_policy_hash": self.policy.content_hash(),
        }


def submit_authority_policy_hash(policy_payload: Mapping[str, object]) -> str:
    encoded = json.dumps(policy_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "utf-8"
    )
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def live_real_order_enabled(settings_obj: object) -> bool:
    return (
        str(getattr(settings_obj, "MODE", "") or "").strip().lower() == "live"
        and not bool(getattr(settings_obj, "LIVE_DRY_RUN", True))
        and bool(getattr(settings_obj, "LIVE_REAL_ORDER_ARMED", False))
    )


def submit_authority_policy_from_settings(settings_obj: object) -> SubmitAuthorityPolicy:
    if live_real_order_enabled(settings_obj):
        return SubmitAuthorityPolicy(
            submit_authority_mode="live_real_order_target_delta_only",
            live_real_order_requires_target_delta=True,
            legacy_lot_native_compat_enabled=False,
            allowed_submit_plan_sources=(TARGET_DELTA_SUBMIT_SOURCE, RESIDUAL_SUBMIT_SOURCE),
            allowed_submit_plan_authorities=tuple(
                sorted(TARGET_DELTA_SUBMIT_AUTHORITIES | RESIDUAL_SUBMIT_AUTHORITIES)
            ),
        )
    if str(getattr(settings_obj, "MODE", "") or "").strip().lower() == "live":
        return SubmitAuthorityPolicy(
            submit_authority_mode="live_dry_run_non_submitting_compat",
            live_real_order_requires_target_delta=False,
            legacy_lot_native_compat_enabled=True,
            allowed_submit_plan_sources=tuple(
                sorted({TARGET_DELTA_SUBMIT_SOURCE, RESIDUAL_SUBMIT_SOURCE} | LEGACY_BUY_SUBMIT_SOURCES)
            ),
            allowed_submit_plan_authorities=tuple(
                sorted(
                    TARGET_DELTA_SUBMIT_AUTHORITIES
                    | RESIDUAL_SUBMIT_AUTHORITIES
                    | LEGACY_BUY_SUBMIT_AUTHORITIES
                )
            ),
        )
    return SubmitAuthorityPolicy(
        submit_authority_mode="paper_research_compat",
        live_real_order_requires_target_delta=False,
        legacy_lot_native_compat_enabled=True,
        allowed_submit_plan_sources=tuple(
            sorted(
                {
                    TARGET_DELTA_SUBMIT_SOURCE,
                    RESIDUAL_SUBMIT_SOURCE,
                    "research_backtest",
                }
                | LEGACY_BUY_SUBMIT_SOURCES
            )
        ),
        allowed_submit_plan_authorities=tuple(
            sorted(
                TARGET_DELTA_SUBMIT_AUTHORITIES
                | RESIDUAL_SUBMIT_AUTHORITIES
                | LEGACY_BUY_SUBMIT_AUTHORITIES
                | {"target_position_delta"}
            )
        ),
    )


def evaluate_submit_authority_policy(
    plan: object,
    *,
    settings_obj: object,
    plan_kind: str,
) -> SubmitAuthorityPolicyDecision:
    policy = submit_authority_policy_from_settings(settings_obj)
    payload = plan.as_dict() if hasattr(plan, "as_dict") else dict(plan or {})
    mode = str(getattr(settings_obj, "MODE", "") or "").strip().lower()
    live_dry_run = bool(getattr(settings_obj, "LIVE_DRY_RUN", True))
    live_real_order_armed = bool(getattr(settings_obj, "LIVE_REAL_ORDER_ARMED", False))
    execution_engine = str(getattr(settings_obj, "EXECUTION_ENGINE", "") or "").strip().lower()
    source = str(payload.get("source") or "").strip()
    authority = str(payload.get("authority") or "").strip()
    side = str(payload.get("side") or "").strip().upper()
    submit_expected = bool(payload.get("submit_expected"))
    proof = str(payload.get("pre_submit_proof_status") or "").strip()
    normalized_kind = str(plan_kind or "").strip().lower()

    def decision(allowed: bool, reason: str) -> SubmitAuthorityPolicyDecision:
        return SubmitAuthorityPolicyDecision(
            allowed=allowed,
            reason=reason,
            policy=policy,
            plan_kind=normalized_kind,
            mode=mode,
            live_dry_run=live_dry_run,
            live_real_order_armed=live_real_order_armed,
            execution_engine=execution_engine,
            source=source,
            authority=authority,
            side=side,
            submit_expected=submit_expected,
            pre_submit_proof_status=proof,
        )

    if mode == "live" and live_dry_run:
        return decision(False, "live_dry_run_non_submitting")
    if policy.live_real_order_requires_target_delta:
        if execution_engine != "target_delta":
            return decision(False, "live_real_order_requires_execution_engine_target_delta")
        if normalized_kind == "target":
            if source != TARGET_DELTA_SUBMIT_SOURCE:
                return decision(False, "live_real_order_target_plan_invalid_source")
            if authority not in TARGET_DELTA_SUBMIT_AUTHORITIES:
                return decision(False, "live_real_order_target_plan_invalid_authority")
            if side not in {"BUY", "SELL"}:
                return decision(False, "live_real_order_target_plan_invalid_side")
            if not submit_expected:
                return decision(False, "live_real_order_target_plan_submit_not_expected")
            if proof != "passed":
                return decision(False, "live_real_order_target_plan_pre_submit_proof_not_passed")
            return decision(True, "allowed_target_delta")
        if normalized_kind == "residual":
            if source != RESIDUAL_SUBMIT_SOURCE:
                return decision(False, "live_real_order_residual_plan_invalid_source")
            if authority not in RESIDUAL_SUBMIT_AUTHORITIES:
                return decision(False, "live_real_order_residual_plan_invalid_authority")
            if side != "SELL":
                return decision(False, "live_real_order_residual_plan_invalid_side")
            if not submit_expected:
                return decision(False, "live_real_order_residual_plan_submit_not_expected")
            if proof != "passed":
                return decision(False, "live_real_order_residual_plan_pre_submit_proof_not_passed")
            return decision(True, "allowed_residual_inventory_policy")
        if normalized_kind == "buy":
            return decision(False, "live_real_order_buy_plan_rejected_target_delta_required")
        if source in LEGACY_BUY_SUBMIT_SOURCES:
            return decision(False, "live_real_order_legacy_source_rejected")
        if authority in LEGACY_BUY_SUBMIT_AUTHORITIES:
            return decision(False, "live_real_order_legacy_authority_rejected")
        return decision(False, "live_real_order_submit_plan_kind_rejected")

    if source not in policy.allowed_submit_plan_sources:
        return decision(False, "submit_plan_source_not_allowed_for_mode")
    if authority not in policy.allowed_submit_plan_authorities:
        return decision(False, "submit_plan_authority_not_allowed_for_mode")
    return decision(True, "allowed_mode_compatibility")


def live_real_order_legacy_buy_submit_plan_error(
    plan: object,
    *,
    settings_obj: object,
) -> str | None:
    policy = submit_authority_policy_from_settings(settings_obj)
    if not policy.live_real_order_requires_target_delta:
        return None
    payload = plan.as_dict() if hasattr(plan, "as_dict") else dict(plan or {})
    side = str(payload.get("side") or "").strip().upper()
    source = str(payload.get("source") or "").strip()
    authority = str(payload.get("authority") or "").strip()
    if side == "BUY" and (
        source != TARGET_DELTA_SUBMIT_SOURCE
        or authority not in TARGET_DELTA_SUBMIT_AUTHORITIES
    ):
        return "live_real_order_buy_plan_rejected_target_delta_required"
    return None
