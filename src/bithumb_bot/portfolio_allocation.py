from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from .canonical_decision import sha256_prefixed
from .portfolio_target import PortfolioTarget
from .risk_decision import (
    RISK_BUDGET_LEGACY_MARKER,
    RISK_BUDGET_SEMANTICS,
    build_risk_decision_artifact,
)
from .risk_contract import RiskPolicy, RiskSnapshot
from .risk_policy_engine import RiskPolicyEngine
from .strategy_preference import StrategyPreference, StrategyPreferenceSet


def _strategy_risk_policy_from_mapping(payload: Mapping[str, object]) -> RiskPolicy:
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
        missing_policy=str(payload.get("missing_policy", "fail_closed_for_promotion") or "fail_closed_for_promotion"),
        source=str(payload.get("source", "runtime_strategy_spec") or "runtime_strategy_spec"),
    )


def _strategy_risk_snapshot_from_mapping(
    payload: Mapping[str, object] | None,
    *,
    strategy_instance_id: str,
) -> RiskSnapshot:
    fields = dict(payload or {})
    return RiskSnapshot(
        evaluation_ts_ms=int(fields.get("evaluation_ts_ms", 0) or 0),
        mark_price=float(fields.get("mark_price", 0.0) or 0.0),
        current_equity=fields.get("current_equity"),  # type: ignore[arg-type]
        baseline_equity=fields.get("baseline_equity"),  # type: ignore[arg-type]
        loss_today=fields.get("loss_today"),  # type: ignore[arg-type]
        current_cash_krw=fields.get("current_cash_krw"),  # type: ignore[arg-type]
        current_asset_qty=fields.get("current_asset_qty"),  # type: ignore[arg-type]
        position_entry_price=fields.get("position_entry_price"),  # type: ignore[arg-type]
        broker_local_mismatch=bool(fields.get("broker_local_mismatch", False)),
        recovery_risk_mismatch_reason=(
            None
            if fields.get("recovery_risk_mismatch_reason") is None
            else str(fields.get("recovery_risk_mismatch_reason"))
        ),
        duplicate_entry=bool(fields.get("duplicate_entry", False)),
        daily_order_count=(
            None if fields.get("daily_order_count") is None else int(fields.get("daily_order_count") or 0)
        ),
        daily_trade_count=(
            None if fields.get("daily_trade_count") is None else int(fields.get("daily_trade_count") or 0)
        ),
        current_drawdown_pct=(
            None
            if fields.get("current_drawdown_pct") is None
            else float(fields.get("current_drawdown_pct") or 0.0)
        ),
        minutes_since_last_loss=(
            None
            if fields.get("minutes_since_last_loss") is None
            else float(fields.get("minutes_since_last_loss") or 0.0)
        ),
        unresolved_order_blocked=bool(fields.get("unresolved_order_blocked", False)),
        unresolved_order_reason_code=str(fields.get("unresolved_order_reason_code", "OK") or "OK"),
        unresolved_order_reason=str(fields.get("unresolved_order_reason", "ok") or "ok"),
        state_source=str(fields.get("state_source", "strategy_risk_snapshot") or "strategy_risk_snapshot"),
        evidence={
            **dict(fields.get("evidence") or {}),
            "strategy_instance_id": strategy_instance_id,
        },
    )


@dataclass(frozen=True)
class StrategyContribution:
    strategy_instance_id: str
    strategy_name: str
    pair: str
    signal_direction: str
    priority: int
    weight: float
    preference_hash: str
    desired_exposure_krw: float | None
    risk_budget_krw: float | None
    max_target_exposure_krw: float | None
    reason: str
    strategy_risk_policy: Mapping[str, object] | None = None
    strategy_risk_snapshot: Mapping[str, object] | None = None
    pre_cap_weighted_target_exposure_krw: float | None = None
    exposure_cap_applied: bool = False
    exposure_cap_source: str = "none"
    risk_budget_semantics: str = RISK_BUDGET_SEMANTICS
    schema_version: int = 1

    def as_dict(self) -> dict[str, object]:
        risk_decision = build_risk_decision_artifact(
            risk_budget_krw=self.risk_budget_krw,
            max_target_exposure_krw=self.max_target_exposure_krw,
            exposure_cap_source=self.exposure_cap_source,
            decision_context="strategy_contribution",
        )
        return {
            "schema_version": int(self.schema_version),
            "strategy_instance_id": self.strategy_instance_id,
            "strategy_name": self.strategy_name,
            "pair": self.pair,
            "signal_direction": self.signal_direction,
            "priority": int(self.priority),
            "weight": float(self.weight),
            "preference_hash": self.preference_hash,
            "desired_exposure_krw": self.desired_exposure_krw,
            "risk_budget_krw": self.risk_budget_krw,
            "max_target_exposure_krw": self.max_target_exposure_krw,
            "pre_cap_weighted_target_exposure_krw": self.pre_cap_weighted_target_exposure_krw,
            "exposure_cap_applied": bool(self.exposure_cap_applied),
            "exposure_cap_source": self.exposure_cap_source,
            "risk_budget_semantics": self.risk_budget_semantics,
            "strategy_risk_policy": (
                None if self.strategy_risk_policy is None else dict(self.strategy_risk_policy)
            ),
            "strategy_risk_snapshot": (
                None if self.strategy_risk_snapshot is None else dict(self.strategy_risk_snapshot)
            ),
            "risk_decision": risk_decision,
            "risk_decision_hash": risk_decision["risk_decision_hash"],
            "risk_budget_legacy_marker": RISK_BUDGET_LEGACY_MARKER,
            "reason": self.reason,
        }

    def content_hash(self) -> str:
        return sha256_prefixed(self.as_dict())


@dataclass(frozen=True)
class PortfolioAllocatorConfig:
    policy_name: str = "deterministic_priority_target_v1"
    policy_version: str = "1"
    target_exposure_krw: float = 0.0
    hold_policy: str = "maintain_previous_target"
    mixed_hold_policy: str = "active_signal_over_hold"
    conflict_policy: str = "fail_closed_equal_priority"
    strategy_priorities: Mapping[str, int] = field(default_factory=dict)
    strategy_weights: Mapping[str, float] = field(default_factory=dict)
    risk_budget_semantics: str = RISK_BUDGET_SEMANTICS
    schema_version: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "target_exposure_krw", float(self.target_exposure_krw))
        object.__setattr__(
            self,
            "strategy_priorities",
            {str(key): int(value) for key, value in dict(self.strategy_priorities).items()},
        )
        object.__setattr__(
            self,
            "strategy_weights",
            {str(key): float(value) for key, value in dict(self.strategy_weights).items()},
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": int(self.schema_version),
            "policy_name": self.policy_name,
            "policy_version": self.policy_version,
            "target_exposure_krw": float(self.target_exposure_krw),
            "hold_policy": self.hold_policy,
            "mixed_hold_policy": self.mixed_hold_policy,
            "conflict_policy": self.conflict_policy,
            "strategy_priorities": dict(sorted(self.strategy_priorities.items())),
            "strategy_weights": dict(sorted(self.strategy_weights.items())),
            "risk_budget_semantics": self.risk_budget_semantics,
        }

    def content_hash(self) -> str:
        return sha256_prefixed(self.as_dict())


@dataclass(frozen=True)
class PortfolioAllocationInput:
    preference_set: StrategyPreferenceSet
    allocator_config: PortfolioAllocatorConfig
    previous_target_exposure_krw: float | None = None
    reference_price: float | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.preference_set, StrategyPreferenceSet):
            raise TypeError("portfolio_allocation_input_requires_preference_set")
        if not isinstance(self.allocator_config, PortfolioAllocatorConfig):
            raise TypeError("portfolio_allocation_input_requires_allocator_config")
        object.__setattr__(
            self,
            "previous_target_exposure_krw",
            None
            if self.previous_target_exposure_krw is None
            else float(self.previous_target_exposure_krw),
        )
        object.__setattr__(
            self,
            "reference_price",
            None if self.reference_price is None else float(self.reference_price),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": int(self.schema_version),
            "preference_set": self.preference_set.as_dict(),
            "allocator_config": self.allocator_config.as_dict(),
            "previous_target_exposure_krw": self.previous_target_exposure_krw,
            "reference_price": self.reference_price,
        }

    def content_hash(self) -> str:
        return sha256_prefixed(self.as_dict())


@dataclass(frozen=True)
class PortfolioAllocationDecision:
    allocation_input_hash: str
    allocator_config_hash: str
    strategy_contribution_hash: str
    targets: tuple[PortfolioTarget, ...]
    contributions: tuple[StrategyContribution, ...]
    conflict_resolution: Mapping[str, object]
    reason: str
    authoritative: bool
    primary_block_reason: str = "none"
    schema_version: int = 1

    def as_dict(self) -> dict[str, object]:
        risk_decision = build_risk_decision_artifact(
            decision_context="portfolio_allocation_decision"
        )
        payload = {
            "schema_version": int(self.schema_version),
            "allocation_input_hash": self.allocation_input_hash,
            "allocator_config_hash": self.allocator_config_hash,
            "strategy_contribution_hash": self.strategy_contribution_hash,
            "targets": [target.as_dict() for target in self.targets],
            "contributions": [contribution.as_dict() for contribution in self.contributions],
            "conflict_resolution": dict(self.conflict_resolution),
            "reason": self.reason,
            "authoritative": bool(self.authoritative),
            "primary_block_reason": self.primary_block_reason,
            "risk_budget_semantics": RISK_BUDGET_SEMANTICS,
            "risk_decision": risk_decision,
            "risk_decision_hash": risk_decision["risk_decision_hash"],
            "risk_budget_legacy_marker": RISK_BUDGET_LEGACY_MARKER,
        }
        payload["allocation_decision_hash"] = sha256_prefixed(
            {key: value for key, value in payload.items() if key != "allocation_decision_hash"}
        )
        return payload

    def content_hash(self) -> str:
        return str(self.as_dict()["allocation_decision_hash"])

    def target_for_pair(self, pair: str) -> PortfolioTarget | None:
        for target in self.targets:
            if target.pair == pair:
                return target
        return None


class SignalAggregator:
    def aggregate(self, preferences: tuple[StrategyPreference, ...]) -> StrategyPreferenceSet:
        if not preferences:
            raise ValueError("strategy_preference_missing")
        return StrategyPreferenceSet(tuple(preferences))


@dataclass(frozen=True)
class PortfolioAllocator:
    config: PortfolioAllocatorConfig

    def allocate(self, allocation_input: PortfolioAllocationInput) -> PortfolioAllocationDecision:
        if not isinstance(allocation_input, PortfolioAllocationInput):
            raise TypeError("portfolio_allocator_requires_typed_input")
        preferences = allocation_input.preference_set.preferences
        if not preferences:
            return self._blocked_decision(
                allocation_input,
                contributions=(),
                reason="strategy_preference_missing",
                pair="",
            )
        contributions = tuple(self._contribution(preference) for preference in preferences)
        contribution_hash = sha256_prefixed([item.as_dict() for item in contributions])
        input_hash = allocation_input.content_hash()
        config_hash = allocation_input.allocator_config.content_hash()
        targets: list[PortfolioTarget] = []
        for pair in sorted({preference.pair for preference in preferences}):
            pair_preferences = tuple(preference for preference in preferences if preference.pair == pair)
            pair_contributions = tuple(item for item in contributions if item.pair == pair)
            targets.append(
                self._allocate_pair(
                    pair=pair,
                    preferences=pair_preferences,
                    contributions=pair_contributions,
                    allocation_input=allocation_input,
                    input_hash=input_hash,
                    config_hash=config_hash,
                    contribution_hash=contribution_hash,
                )
            )
        authoritative = all(bool(target.authoritative) for target in targets)
        primary_block_reason = "none"
        for target in targets:
            if not bool(target.authoritative):
                primary_block_reason = target.fail_closed_reason
                break
        conflict_resolution = {
            "policy": self.config.conflict_policy,
            "target_count": len(targets),
            "blocked_target_count": sum(1 for target in targets if not bool(target.authoritative)),
            "conflict_count": sum(
                int(target.conflict_resolution.get("conflict_count") or 0) for target in targets
            ),
        }
        return PortfolioAllocationDecision(
            allocation_input_hash=input_hash,
            allocator_config_hash=config_hash,
            strategy_contribution_hash=contribution_hash,
            targets=tuple(targets),
            contributions=contributions,
            conflict_resolution=conflict_resolution,
            reason="allocated" if authoritative else primary_block_reason,
            authoritative=authoritative,
            primary_block_reason=primary_block_reason,
        )

    def _contribution(self, preference: StrategyPreference) -> StrategyContribution:
        instance_id = preference.strategy_instance_id or preference.strategy_name
        return StrategyContribution(
            strategy_instance_id=instance_id,
            strategy_name=preference.strategy_name,
            pair=preference.pair,
            signal_direction=preference.signal_direction,
            priority=int(self.config.strategy_priorities.get(instance_id, 100)),
            weight=float(self.config.strategy_weights.get(instance_id, preference.desired_weight or 1.0)),
            preference_hash=preference.content_hash(),
            desired_exposure_krw=preference.desired_exposure_krw,
            risk_budget_krw=preference.risk_budget_krw,
            max_target_exposure_krw=preference.max_target_exposure_krw,
            strategy_risk_policy=preference.risk_policy,
            strategy_risk_snapshot=preference.risk_snapshot,
            reason=preference.reason,
        )

    def _allocate_pair(
        self,
        *,
        pair: str,
        preferences: tuple[StrategyPreference, ...],
        contributions: tuple[StrategyContribution, ...],
        allocation_input: PortfolioAllocationInput,
        input_hash: str,
        config_hash: str,
        contribution_hash: str,
    ) -> PortfolioTarget:
        if not preferences:
            return self._blocked_target(pair, input_hash, config_hash, contribution_hash, "strategy_preference_missing")
        best_priority = min(item.priority for item in contributions)
        top = tuple(item for item in contributions if item.priority == best_priority)
        top_signals = {item.signal_direction for item in top}
        conflict_count = 1 if {"BUY", "SELL"}.issubset(top_signals) else 0
        conflict_resolution = {
            "policy": self.config.conflict_policy,
            "mixed_hold_policy": self.config.mixed_hold_policy,
            "selected_priority": best_priority,
            "selected_strategy_instance_ids": [item.strategy_instance_id for item in top],
            "selected_strategies": [item.strategy_name for item in top],
            "selected_signals": sorted(top_signals),
            "conflict_count": conflict_count,
        }
        if conflict_count:
            return self._blocked_target(
                pair,
                input_hash,
                config_hash,
                contribution_hash,
                "conflicting_equal_priority_signals",
                conflict_resolution=conflict_resolution,
            )
        active_top_signals = sorted(signal for signal in top_signals if signal != "HOLD")
        selected_signal = active_top_signals[0] if active_top_signals else "HOLD"
        conflict_resolution["selected_signal"] = selected_signal
        if selected_signal == "BUY":
            buy_contributions = tuple(item for item in top if item.signal_direction == "BUY")
            risk_block = self._selected_strategy_risk_block(buy_contributions)
            if risk_block is not None:
                conflict_resolution.update(risk_block)
                return self._blocked_target(
                    pair,
                    input_hash,
                    config_hash,
                    contribution_hash,
                    str(risk_block["strategy_risk_block_reason_code"]),
                    conflict_resolution=conflict_resolution,
                )
            target_exposure, cap_audit = self._buy_target_exposure(buy_contributions)
            conflict_resolution.update(cap_audit)
            reason = "buy_weighted_target_from_allocator"
        elif selected_signal == "SELL":
            target_exposure = 0.0
            reason = "sell_target_zero_exposure"
        elif selected_signal == "HOLD":
            if allocation_input.previous_target_exposure_krw is None:
                return self._blocked_target(
                    pair,
                    input_hash,
                    config_hash,
                    contribution_hash,
                    "hold_missing_previous_target_exposure",
                    conflict_resolution=conflict_resolution,
                )
            target_exposure = max(0.0, float(allocation_input.previous_target_exposure_krw))
            reason = "hold_maintains_previous_target"
        else:
            return self._blocked_target(
                pair,
                input_hash,
                config_hash,
                contribution_hash,
                "unsupported_strategy_preference_signal",
                conflict_resolution=conflict_resolution,
            )
        target_qty = (
            None
            if allocation_input.reference_price is None or float(allocation_input.reference_price) <= 0.0
            else float(target_exposure) / float(allocation_input.reference_price)
        )
        return PortfolioTarget(
            pair=pair,
            target_exposure_krw=target_exposure,
            target_qty=target_qty,
            allocator_policy_name=self.config.policy_name,
            allocator_policy_version=self.config.policy_version,
            allocator_config_hash=config_hash,
            strategy_contribution_hash=contribution_hash,
            allocation_input_hash=input_hash,
            reason=reason,
            conflict_resolution=conflict_resolution,
            authoritative=True,
            fail_closed_reason="none",
        )

    def _buy_target_exposure(self, contributions: tuple[StrategyContribution, ...]) -> tuple[float, dict[str, object]]:
        if not contributions:
            target = max(0.0, float(self.config.target_exposure_krw))
            risk_decision = build_risk_decision_artifact(
                decision_context="portfolio_allocator_default_target"
            )
            return target, {
                "pre_cap_weighted_target_exposure_krw": target,
                "exposure_cap_krw": None,
                "exposure_cap_applied": False,
                "exposure_cap_source": "none",
                "risk_budget_semantics": RISK_BUDGET_SEMANTICS,
                "risk_decision": risk_decision,
                "risk_decision_hash": risk_decision["risk_decision_hash"],
                "risk_budget_legacy_marker": RISK_BUDGET_LEGACY_MARKER,
            }
        weighted_total = 0.0
        weight_total = 0.0
        exposure_cap_total = 0.0
        exposure_cap_present = False
        for item in contributions:
            weight = max(0.0, float(item.weight))
            exposure = (
                float(item.desired_exposure_krw)
                if item.desired_exposure_krw is not None
                else max(0.0, float(self.config.target_exposure_krw))
            )
            weighted_total += exposure * weight
            weight_total += weight
            if item.max_target_exposure_krw is not None:
                exposure_cap_present = True
                exposure_cap_total += max(0.0, float(item.max_target_exposure_krw))
        target = weighted_total / weight_total if weight_total > 0.0 else 0.0
        pre_cap = target
        cap_applied = False
        if exposure_cap_present:
            capped = min(target, exposure_cap_total)
            cap_applied = capped < target
            target = capped
        risk_decision = build_risk_decision_artifact(
            max_target_exposure_krw=(exposure_cap_total if exposure_cap_present else None),
            exposure_cap_source="max_target_exposure_krw" if exposure_cap_present else "none",
            decision_context="portfolio_allocator_buy_target",
        )
        return max(0.0, float(target)), {
            "pre_cap_weighted_target_exposure_krw": max(0.0, float(pre_cap)),
            "exposure_cap_krw": exposure_cap_total if exposure_cap_present else None,
            "exposure_cap_applied": cap_applied,
            "exposure_cap_source": "max_target_exposure_krw" if exposure_cap_present else "none",
            "risk_budget_semantics": RISK_BUDGET_SEMANTICS,
            "risk_decision": risk_decision,
            "risk_decision_hash": risk_decision["risk_decision_hash"],
            "risk_budget_legacy_marker": RISK_BUDGET_LEGACY_MARKER,
        }

    def _selected_strategy_risk_block(
        self,
        contributions: tuple[StrategyContribution, ...],
    ) -> dict[str, object] | None:
        for item in contributions:
            if not isinstance(item.strategy_risk_policy, Mapping):
                continue
            policy = _strategy_risk_policy_from_mapping(item.strategy_risk_policy)
            snapshot = _strategy_risk_snapshot_from_mapping(
                item.strategy_risk_snapshot,
                strategy_instance_id=item.strategy_instance_id,
            )
            decision = RiskPolicyEngine(policy).evaluate_pre_decision(snapshot)
            if decision.status != "ALLOW":
                return {
                    "strategy_risk_policy_blocked": True,
                    "strategy_risk_blocked_instance_id": item.strategy_instance_id,
                    "strategy_risk_block_reason_code": decision.reason_code,
                    "strategy_risk_decision": decision.as_dict(),
                    "strategy_risk_decision_hash": decision.risk_decision_hash,
                    "risk_decision_hash": decision.risk_decision_hash,
                }
        return None

    def _blocked_decision(
        self,
        allocation_input: PortfolioAllocationInput,
        *,
        contributions: tuple[StrategyContribution, ...],
        reason: str,
        pair: str,
    ) -> PortfolioAllocationDecision:
        input_hash = allocation_input.content_hash()
        config_hash = allocation_input.allocator_config.content_hash()
        contribution_hash = sha256_prefixed([item.as_dict() for item in contributions])
        target = self._blocked_target(pair, input_hash, config_hash, contribution_hash, reason)
        return PortfolioAllocationDecision(
            allocation_input_hash=input_hash,
            allocator_config_hash=config_hash,
            strategy_contribution_hash=contribution_hash,
            targets=(target,),
            contributions=contributions,
            conflict_resolution={"policy": self.config.conflict_policy, "conflict_count": 0},
            reason=reason,
            authoritative=False,
            primary_block_reason=reason,
        )

    def _blocked_target(
        self,
        pair: str,
        input_hash: str,
        config_hash: str,
        contribution_hash: str,
        reason: str,
        *,
        conflict_resolution: Mapping[str, object] | None = None,
    ) -> PortfolioTarget:
        return PortfolioTarget(
            pair=pair,
            target_exposure_krw=None,
            target_qty=None,
            allocator_policy_name=self.config.policy_name,
            allocator_policy_version=self.config.policy_version,
            allocator_config_hash=config_hash,
            strategy_contribution_hash=contribution_hash,
            allocation_input_hash=input_hash,
            reason=reason,
            conflict_resolution=dict(conflict_resolution or {"policy": self.config.conflict_policy}),
            authoritative=False,
            fail_closed_reason=reason,
        )
