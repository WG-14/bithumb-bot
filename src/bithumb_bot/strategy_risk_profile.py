from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Mapping

from .canonical_decision import canonical_payload_hash
from .risk_contract import RiskPolicy


RiskProfileSource = Literal[
    "approved_runtime_profile",
    "approved_risk_profile",
    "runtime_strategy_spec_fixture",
    "research_missing_policy_explicit",
]
RiskEnforcementMode = Literal["telemetry", "enforced"]
MissingRiskPolicyBehavior = Literal["fail_closed_for_live", "disabled_explicit"]


def risk_policy_from_mapping(payload: Mapping[str, object]) -> RiskPolicy:
    return RiskPolicy(
        schema_version=int(payload.get("schema_version", 1) or 1),
        max_daily_loss_krw=float(payload.get("max_daily_loss_krw", 0.0) or 0.0),
        max_position_loss_pct=float(payload.get("max_position_loss_pct", 0.0) or 0.0),
        max_daily_order_count=int(payload.get("max_daily_order_count", 0) or 0),
        max_trade_count_per_day=int(payload.get("max_trade_count_per_day", 0) or 0),
        max_drawdown_pct=float(payload.get("max_drawdown_pct", 0.0) or 0.0),
        cooldown_after_loss_min=int(payload.get("cooldown_after_loss_min", 0) or 0),
        kill_switch=bool(payload.get("kill_switch", False)),
        max_open_positions=int(payload.get("max_open_positions", 1) or 1),
        unresolved_order_policy=str(payload.get("unresolved_order_policy", "block") or "block"),
        policy_status=str(payload.get("policy_status", "enabled") or "enabled"),
        missing_policy=str(payload.get("missing_policy", "fail_closed_for_live") or "fail_closed_for_live"),
        source=str(payload.get("source", "strategy_risk_profile") or "strategy_risk_profile"),
    )


@dataclass(frozen=True)
class StrategyRiskProfile:
    schema_version: int
    strategy_instance_id: str
    strategy_name: str
    pair: str
    interval: str
    policy: RiskPolicy
    risk_policy_hash: str
    risk_profile_source: RiskProfileSource
    approved_risk_profile_path: str | None
    approved_runtime_profile_path: str | None
    approved_risk_profile_hash: str | None
    approved_runtime_profile_hash: str | None
    enforcement_mode: RiskEnforcementMode
    missing_policy_behavior: MissingRiskPolicyBehavior

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": int(self.schema_version),
            "strategy_instance_id": self.strategy_instance_id,
            "strategy_name": self.strategy_name,
            "pair": self.pair,
            "interval": self.interval,
            "risk_policy": self.policy.as_dict(),
            "risk_policy_hash": self.risk_policy_hash,
            "risk_profile_source": self.risk_profile_source,
            "approved_risk_profile_path": self.approved_risk_profile_path,
            "approved_runtime_profile_path": self.approved_runtime_profile_path,
            "approved_risk_profile_hash": self.approved_risk_profile_hash,
            "approved_runtime_profile_hash": self.approved_runtime_profile_hash,
            "enforcement_mode": self.enforcement_mode,
            "missing_policy_behavior": self.missing_policy_behavior,
        }

    def profile_hash(self) -> str:
        return canonical_payload_hash(self.as_dict())


def strategy_risk_profile_from_profile_payload(
    *,
    strategy_instance_id: str,
    strategy_name: str,
    pair: str,
    interval: str,
    profile_payload: Mapping[str, object] | None,
    approved_runtime_profile_path: str | None,
    approved_runtime_profile_hash: str | None,
    inline_risk_policy: Mapping[str, object] | None = None,
    declared_risk_policy_hash: str | None = None,
    risk_profile_source: str | None = None,
    enforcement_mode: str | None = None,
    missing_policy_behavior: str | None = None,
    live_like: bool = False,
    live_real_order: bool = False,
) -> StrategyRiskProfile | None:
    if live_like and inline_risk_policy is not None:
        raise RuntimeError(f"inline_risk_policy_rejected_for_live_authority:{strategy_name}")

    profile = dict(profile_payload or {})
    raw_policy = profile.get("risk_policy")
    if not isinstance(raw_policy, Mapping):
        if inline_risk_policy is None:
            if live_like:
                raise RuntimeError(f"strategy_risk_profile_missing_for_live_strategy:{strategy_name}")
            policy = RiskPolicy(
                schema_version=1,
                policy_status="disabled_explicit",
                missing_policy="disabled_explicit",
                source="research_missing_policy_explicit",
            )
            return StrategyRiskProfile(
                schema_version=1,
                strategy_instance_id=str(strategy_instance_id),
                strategy_name=str(strategy_name),
                pair=str(pair),
                interval=str(interval),
                policy=policy,
                risk_policy_hash=policy.policy_hash(),
                risk_profile_source="research_missing_policy_explicit",
                approved_risk_profile_path=None,
                approved_runtime_profile_path=approved_runtime_profile_path,
                approved_risk_profile_hash=None,
                approved_runtime_profile_hash=approved_runtime_profile_hash,
                enforcement_mode="telemetry",
                missing_policy_behavior="disabled_explicit",
            )
        raw_policy = inline_risk_policy

    policy = risk_policy_from_mapping(raw_policy)
    policy_hash = policy.policy_hash()
    declared_hash = str(declared_risk_policy_hash or profile.get("risk_policy_hash") or "").strip()
    if declared_hash and declared_hash != policy_hash:
        raise RuntimeError(f"strategy_risk_policy_hash_mismatch:{strategy_name}")

    source = str(risk_profile_source or profile.get("risk_profile_source") or "").strip()
    if not source:
        source = "approved_runtime_profile" if profile_payload is not None else "runtime_strategy_spec_fixture"
    if source not in {
        "approved_runtime_profile",
        "approved_risk_profile",
        "runtime_strategy_spec_fixture",
        "research_missing_policy_explicit",
    }:
        raise RuntimeError(f"strategy_risk_profile_source_unsupported:{strategy_name}:{source}")
    if live_like and source == "runtime_strategy_spec_fixture":
        raise RuntimeError(f"runtime_strategy_spec_risk_profile_rejected_for_live:{strategy_name}")

    mode = str(enforcement_mode or profile.get("risk_enforcement_mode") or "enforced").strip().lower()
    if mode not in {"telemetry", "enforced"}:
        raise RuntimeError(f"strategy_risk_enforcement_mode_unsupported:{strategy_name}:{mode}")
    if live_real_order and mode != "enforced":
        raise RuntimeError(f"strategy_risk_enforcement_required_for_live_real_order:{strategy_name}")
    if live_real_order and policy.policy_status == "disabled_explicit":
        raise RuntimeError(
            f"strategy_risk_policy_disabled_rejected_for_live_real_order:{strategy_name}"
        )

    missing = str(
        missing_policy_behavior
        or profile.get("missing_risk_policy_behavior")
        or policy.missing_policy
        or "fail_closed_for_live"
    ).strip()
    if missing not in {"fail_closed_for_live", "disabled_explicit"}:
        raise RuntimeError(f"strategy_risk_missing_policy_behavior_unsupported:{strategy_name}:{missing}")
    if live_like and missing != "fail_closed_for_live":
        raise RuntimeError(f"strategy_risk_missing_policy_must_fail_closed_for_live:{strategy_name}")

    return StrategyRiskProfile(
        schema_version=1,
        strategy_instance_id=str(strategy_instance_id),
        strategy_name=str(strategy_name),
        pair=str(pair),
        interval=str(interval),
        policy=policy,
        risk_policy_hash=policy_hash,
        risk_profile_source=source,  # type: ignore[arg-type]
        approved_risk_profile_path=(
            str(profile.get("approved_risk_profile_path") or "").strip() or None
        ),
        approved_runtime_profile_path=approved_runtime_profile_path,
        approved_risk_profile_hash=(
            str(profile.get("approved_risk_profile_hash") or "").strip() or None
        ),
        approved_runtime_profile_hash=approved_runtime_profile_hash,
        enforcement_mode=mode,  # type: ignore[arg-type]
        missing_policy_behavior=missing,  # type: ignore[arg-type]
    )
