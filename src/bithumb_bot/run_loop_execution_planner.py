from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Callable, Mapping

from .config import settings
from .canonical_decision import order_rules_snapshot_payload
from .db_core import (
    load_strategy_virtual_target_state,
    load_target_position_state,
)
from .decision_envelope import DecisionEnvelope, _thaw_mapping
from .decision_equivalence import sha256_prefixed
from .execution_order_rules import resolve_execution_order_rules
from .execution_plan_batch import ExecutionPlanBatch, PairExecutionPlan
from .experiment_execution_contract import POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT
from .execution_service import (
    ExecutionDecisionSummary,
    ExecutionReadinessPlanningInput,
    ExecutionSubmitPlan,
    ExecutionTargetPlanningInput,
    H74_SUBMIT_SEMANTIC_FIELDS,
    TypedExecutionPlanningInput,
    build_typed_execution_decision_summary,
)
from .portfolio_allocation import (
    PortfolioAllocationInput,
    PortfolioAllocator,
    PortfolioAllocatorConfig,
    SignalAggregator,
)
from .strategy_preference import strategy_decision_to_preference
from .risk_policy_engine import RiskPolicyEngine
from .strategy_risk_state import StrategyRiskStateProvider, missing_required_risk_state
from .runtime_readiness import compute_runtime_readiness_snapshot
from .runtime_strategy_set import (
    MULTI_PAIR_RUNTIME_UNSUPPORTED_REASON,
    RUNTIME_SCOPE_MODE,
    ProfileAuthorityContext,
    RuntimeDecisionRequestBuilder,
    RuntimeStrategyDecisionResultBundle,
    RuntimeStrategySet,
    derive_strategy_instance_id,
    runtime_scope_contract,
    runtime_strategy_set_manifest_hash,
)
from .strategy_risk_profile import strategy_risk_profile_from_profile_payload
from .strategy_policy_contract import StrategyDecisionV2
from .strategy_performance import evaluate_strategy_performance_gate
from .h74_cycle_state import (
    build_h74_cycle_id,
    load_h74_cycle_inventory,
    load_open_h74_cycle_inventories,
)
from .h74_observation import (
    H74_SOURCE_OBSERVATION_AUTHORITY_ENV,
    verify_h74_source_observation_authority,
    h74_source_runtime_values_from_settings,
)
from .h74_startup_gate import evaluate_h74_startup_gate
from .target_position import (
    STARTUP_TARGET_SOURCE_BROKER_POSITION_ADOPTION,
    STARTUP_TARGET_SOURCE_POLICY_INITIALIZATION,
    STARTUP_TARGET_SOURCE_TRUE_DUST_FLAT_INITIALIZATION,
    TARGET_POLICY_ADOPT_EXISTING_BROKER_POSITION,
    TARGET_POLICY_INITIALIZE_FLAT_TARGET,
    TARGET_POLICY_INITIALIZE_TRUE_DUST_FLAT,
    TARGET_POLICY_USE_EXISTING_TARGET,
    resolve_startup_target_position_policy,
)
from .virtual_target_state import (
    build_strategy_virtual_lifecycle_skipped_artifact,
    build_strategy_virtual_lifecycle_transition_artifact,
    evolve_strategy_virtual_target_state,
)


READINESS_CONTEXT_KEYS = (
    "residual_inventory_mode",
    "residual_inventory_state",
    "residual_inventory_policy_allows_run",
    "residual_inventory_policy_allows_buy",
    "residual_inventory_policy_allows_sell",
    "residual_inventory_qty",
    "residual_inventory_notional_krw",
    "residual_inventory_exchange_sellable",
    "total_effective_exposure_qty",
    "total_effective_exposure_notional_krw",
    "residual_sell_candidate",
    "unresolved_open_order_count",
    "submit_unknown_count",
    "target_policy_action",
    "target_origin",
    "target_adoption_reason",
    "target_adopted_broker_qty",
    "target_adopted_exposure_krw",
    "target_startup_policy_state",
    "target_existing_state_present",
    "target_missing_state_resolution",
    "target_closeout_requested",
    "target_strategy_signal_source",
    "position_mode",
    "hold_policy",
    "authority_hash",
    "authority_parameter_hash",
    "source_artifact_hash",
    "strategy_instance_id",
    "cycle_id",
    "h74_cycle_id",
    "remaining_cycle_qty",
    "h74_remaining_cycle_qty",
    "h74_cycle_inventory",
    "locked_exit_qty",
    "h74_cycle_inventory_error",
    "h74_open_cycle_count",
    "partial_fill_policy",
    "h74_startup_gate_status",
    "h74_startup_gate_reason_code",
    "startup_gate_hash",
    "startup_gate",
    "contract_hash",
    "experiment_execution_contract",
    "cash_available",
)


def _no_broker_provider() -> object | None:
    return None


def _load_h74_source_authority_payload(settings_obj: object) -> dict[str, object]:
    authority_path = (
        str(getattr(settings_obj, H74_SOURCE_OBSERVATION_AUTHORITY_ENV, "") or "").strip()
    )
    if not authority_path:
        return {}
    with Path(authority_path).expanduser().open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("h74_source_observation_authority_payload_not_object")
    verify_h74_source_observation_authority(
        payload,
        runtime_values=h74_source_runtime_values_from_settings(settings_obj),
    )
    return payload


def _h74_authority_planning_fields(settings_obj: object) -> dict[str, object]:
    authority = _load_h74_source_authority_payload(settings_obj)
    if not authority:
        return {}
    bound = dict(authority.get("hash_bound_parameters") or {})
    return {
        "position_mode": str(authority.get("position_mode") or bound.get("position_mode") or ""),
        "hold_policy": str(authority.get("hold_policy") or bound.get("hold_policy") or "max_holding_time"),
        "authority_hash": str(authority.get("authority_content_hash") or ""),
        "h74_source_authority_hash": str(authority.get("authority_content_hash") or ""),
        "authority_parameter_hash": str(authority.get("authority_parameter_hash") or ""),
        "source_artifact_hash": str(
            bound.get("source_candidate_artifact_hash") or bound.get("source_artifact_hash") or ""
        ),
        "h74_source_authority": {
            "artifact_type": authority.get("artifact_type"),
            "authority_content_hash": authority.get("authority_content_hash"),
            "authority_parameter_hash": authority.get("authority_parameter_hash"),
            "hash_bound_parameters": bound,
        },
        "strategy_instance_id": str(authority.get("strategy_instance_id") or bound.get("strategy_instance_id") or ""),
        "residual_inventory_mode": str(
            authority.get("residual_inventory_mode")
            or bound.get("residual_inventory_mode")
            or "block_executable_residual"
        ),
        "partial_fill_policy": str(
            authority.get("partial_fill_policy")
            or bound.get("partial_fill_policy")
            or "accumulate_cycle_fills"
        ),
    }


def _h74_entry_cycle_fields(*, planning_context: Mapping[str, object], updated_ts: int) -> dict[str, object]:
    authority_hash = str(planning_context.get("authority_hash") or "").strip()
    strategy_instance_id = str(planning_context.get("strategy_instance_id") or "").strip()
    if not authority_hash or not strategy_instance_id:
        return {}
    entry_client_order_id = f"h74_entry_plan_{int(updated_ts)}"
    cycle_id = build_h74_cycle_id(
        strategy_instance_id=strategy_instance_id,
        entry_client_order_id=entry_client_order_id,
        authority_hash=authority_hash,
    )
    return {
        "cycle_id": cycle_id,
        "h74_cycle_id": cycle_id,
        "h74_entry_plan_client_order_id": entry_client_order_id,
    }


def _inject_h74_cycle_inventory(
    conn: object,
    *,
    readiness_payload: dict[str, object],
    planning_context: Mapping[str, object],
) -> dict[str, object]:
    cycle_id = str(
        readiness_payload.get("h74_cycle_id")
        or readiness_payload.get("cycle_id")
        or planning_context.get("h74_cycle_id")
        or planning_context.get("cycle_id")
        or ""
    ).strip()
    if not cycle_id:
        strategy_instance_id = str(
            readiness_payload.get("strategy_instance_id")
            or planning_context.get("strategy_instance_id")
            or ""
        ).strip()
        authority_hash = str(
            readiness_payload.get("authority_hash")
            or readiness_payload.get("h74_source_authority_hash")
            or planning_context.get("authority_hash")
            or planning_context.get("h74_source_authority_hash")
            or ""
        ).strip()
        pair = str(
            readiness_payload.get("runtime_pair")
            or planning_context.get("runtime_pair")
            or getattr(settings, "PAIR", "")
            or ""
        ).strip()
        if not strategy_instance_id or not authority_hash or not pair:
            return readiness_payload
        inventories = load_open_h74_cycle_inventories(
            conn,
            strategy_instance_id=strategy_instance_id,
            authority_hash=authority_hash,
            pair=pair,
        )
        if len(inventories) > 1:
            return {
                **readiness_payload,
                "h74_cycle_inventory_error": "multiple_open_h74_cycles",
                "h74_open_cycle_count": len(inventories),
            }
        inventory = inventories[0] if inventories else None
    else:
        inventory = load_h74_cycle_inventory(conn, cycle_id=cycle_id)
    if inventory is None:
        return readiness_payload
    inventory_payload = inventory.as_dict()
    return {
        **readiness_payload,
        "cycle_id": inventory.cycle_id,
        "h74_cycle_id": inventory.cycle_id,
        "authority_hash": inventory.authority_hash,
        "strategy_instance_id": inventory.strategy_instance_id,
        "locked_exit_qty": inventory.locked_exit_qty,
        "remaining_cycle_qty": inventory.remaining_cycle_qty,
        "h74_remaining_cycle_qty": inventory.remaining_cycle_qty,
        "h74_cycle_inventory": inventory_payload,
    }


def _inject_h74_startup_gate(
    *,
    readiness_payload: dict[str, object],
    target_state: Mapping[str, object],
    authority_fields: Mapping[str, object],
) -> dict[str, object]:
    gate = evaluate_h74_startup_gate(
        readiness_payload=readiness_payload,
        target_state=target_state,
        authority=authority_fields,
    )
    gate_payload = gate.as_dict()
    return {
        **readiness_payload,
        "h74_startup_gate_status": gate.status,
        "h74_startup_gate_reason_code": gate.reason_code,
        "startup_gate_hash": gate_payload["startup_gate_hash"],
        "startup_gate": gate_payload,
    }


@dataclass(frozen=True)
class ExecutionPlanningResult:
    context: dict[str, object]
    execution_decision: dict[str, object]
    execution_decision_summary: ExecutionDecisionSummary | None
    readiness_payload: dict[str, object]
    target_policy_metadata: dict[str, object]
    planning_error: str | None = None
    failure_phase: str | None = None
    failure_subphase: str | None = None
    failure_reason_code: str | None = None
    exception_type: str | None = None
    exception_message: str | None = None


@dataclass(frozen=True)
class ExecutionPlanBundle:
    summary: ExecutionDecisionSummary | None
    submit_plan: ExecutionSubmitPlan | None
    persistence_context: dict[str, object]
    readiness_payload: dict[str, object]
    target_policy_metadata: dict[str, object]
    planning_error: str | None = None
    status: "ExecutionPlanStatus | None" = None
    execution_plan_batch: ExecutionPlanBatch | None = None

    def as_dict(self) -> dict[str, object]:
        submit_plan = None if self.submit_plan is None else {
            **self.submit_plan.as_dict(),
            "submit_plan_hash": self.submit_plan.content_hash(),
        }
        summary = None if self.summary is None else self.summary.as_dict()
        status = None if self.status is None else self.status.as_dict()
        batch_payload = None if self.execution_plan_batch is None else self.execution_plan_batch.as_dict()
        return {
            "schema_version": 1,
            "authority_label": "ExecutionPlanBundle",
            "summary_authority": "ExecutionDecisionSummary" if self.summary is not None else "missing",
            "submit_plan_authority": (
                "derived_from_execution_plan_batch_pair_plan"
                if self.submit_plan is not None and self.execution_plan_batch is not None
                else "ExecutionSubmitPlan"
                if self.submit_plan is not None
                else "none"
            ),
            "execution_plan_batch_authority": (
                "ExecutionPlanBatch" if self.execution_plan_batch is not None else "missing"
            ),
            "summary": summary,
            "primary_submit_plan": submit_plan,
            "execution_plan_batch": batch_payload,
            "execution_plan_batch_hash": None
            if self.execution_plan_batch is None
            else self.execution_plan_batch.content_hash(),
            "execution_plan_batch_id": None
            if self.execution_plan_batch is None
            else self.execution_plan_batch.batch_id,
            "execution_submit_plan_hash": None
            if self.submit_plan is None
            else self.submit_plan.content_hash(),
            "status": status,
            "planning_error": self.planning_error,
            "persistence_context_hash": sha256_prefixed(self.persistence_context),
            "readiness_payload_hash": sha256_prefixed(self.readiness_payload),
            "target_policy_metadata": dict(self.target_policy_metadata),
            "target_policy_metadata_hash": sha256_prefixed(self.target_policy_metadata),
        }

    def content_hash(self) -> str:
        return sha256_prefixed(self.as_dict())


@dataclass(frozen=True)
class ExecutionPlanStatus:
    status: str
    reason_code: str
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "reason_code": self.reason_code,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ExecutionPlanningInput:
    strategy_decision: StrategyDecisionV2
    candle_ts: int
    market_price: float
    base_observability_context: Mapping[str, object]
    replay_fingerprint: Mapping[str, object]
    boundary: Mapping[str, object]
    policy_hashes: Mapping[str, object]

    @classmethod
    def from_envelope(cls, envelope: DecisionEnvelope) -> "ExecutionPlanningInput":
        return cls(
            strategy_decision=envelope.strategy_decision,
            candle_ts=envelope.candle_ts,
            market_price=envelope.market_price,
            base_observability_context=envelope.base_context,
            replay_fingerprint=envelope.replay_fingerprint,
            boundary=envelope.boundary,
            policy_hashes=envelope._policy_hashes_as_dict(),
        )

    @classmethod
    def from_runtime_result_bundle(
        cls,
        result_bundle: RuntimeStrategyDecisionResultBundle,
    ) -> "ExecutionPlanningInput":
        """Build a multi-strategy planning boundary without representative signal authority."""
        if not result_bundle.results:
            raise ValueError("runtime_strategy_result_bundle_empty")
        candle_ts = int(result_bundle.candle_ts)
        market_price = float(result_bundle.market_price)
        for result in result_bundle.results:
            if int(result.candle_ts) != candle_ts:
                raise ValueError("runtime_strategy_results_must_share_candle")
            if abs(float(result.market_price) - market_price) > 1e-9:
                raise ValueError("runtime_strategy_results_must_share_market_price")
        observability_template = result_bundle.results[0].decision
        safe_decision = replace(
            observability_template,
            strategy_name="multi_strategy",
            raw_signal="HOLD",
            raw_reason="multi_strategy_allocator_pending",
            entry_signal="HOLD",
            entry_reason="multi_strategy_allocator_pending",
            exit_signal="HOLD",
            exit_reason="multi_strategy_allocator_pending",
            final_signal="HOLD",
            final_reason="multi_strategy_allocator_pending",
        )
        bundle_hash = result_bundle.content_hash()
        return cls(
            strategy_decision=safe_decision,
            candle_ts=candle_ts,
            market_price=market_price,
            base_observability_context={
                "strategy": "multi_strategy",
                "strategy_name": "multi_strategy",
                "runtime_strategy_decision_bundle_hash": bundle_hash,
                "runtime_strategy_decision_bundle_authority": "typed_bundle",
                "execution_signal_authority": "portfolio_allocator_portfolio_target",
            },
            replay_fingerprint={
                "schema_version": 1,
                "candle_ts": candle_ts,
                "market_price": market_price,
                "runtime_strategy_decision_bundle_hash": bundle_hash,
            },
            boundary={
                "decision_boundary_phase": "multi_strategy_allocator_planning",
                "signal_authority": "portfolio_allocator_portfolio_target",
            },
            policy_hashes={},
        )

    @property
    def raw_signal(self) -> str:
        return str(self.strategy_decision.raw_signal or "HOLD").upper()

    @property
    def final_signal(self) -> str:
        return str(self.strategy_decision.final_signal or "HOLD").upper()

    @property
    def final_reason(self) -> str:
        return str(self.strategy_decision.final_reason or "")


@dataclass(frozen=True)
class ExecutionAuthorityEnvelope:
    """Typed execution-planning authority derived before persistence context exists."""

    planning_input: ExecutionPlanningInput
    readiness: ExecutionReadinessPlanningInput
    target: ExecutionTargetPlanningInput
    target_policy_metadata: Mapping[str, object] = field(default_factory=dict)
    performance_gate_result: object | None = None
    observability_context: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.readiness, ExecutionReadinessPlanningInput):
            raise TypeError("typed_execution_readiness_missing")
        if not isinstance(self.target, ExecutionTargetPlanningInput):
            raise TypeError("typed_execution_target_missing")
        object.__setattr__(
            self,
            "target_policy_metadata",
            {str(key): value for key, value in dict(self.target_policy_metadata).items()},
        )
        object.__setattr__(
            self,
            "observability_context",
            {str(key): value for key, value in dict(self.observability_context).items()},
        )

    @property
    def strategy_decision(self) -> StrategyDecisionV2:
        return self.planning_input.strategy_decision

    def typed_planning_input(self) -> TypedExecutionPlanningInput:
        return TypedExecutionPlanningInput(
            strategy_decision=self.planning_input.strategy_decision,
            candle_ts=self.planning_input.candle_ts,
            market_price=self.planning_input.market_price,
            readiness=self.readiness,
            target=self.target,
            observability_context=self.observability_context,
        )


def _allocator_target_exposure_authority(settings_obj: object = settings) -> tuple[float, str, bool]:
    explicit = getattr(settings_obj, "TARGET_EXPOSURE_KRW", None)
    if explicit is not None:
        try:
            return max(0.0, float(explicit)), "TARGET_EXPOSURE_KRW", True
        except (TypeError, ValueError):
            pass
    return (
        max(0.0, float(getattr(settings_obj, "MAX_ORDER_KRW", 0.0) or 0.0)),
        "MAX_ORDER_KRW",
        True,
    )


def _allocator_target_exposure_krw(settings_obj: object = settings) -> float:
    exposure, _source, _legacy = _allocator_target_exposure_authority(settings_obj)
    return exposure


def _strict_target_exposure_required(
    *,
    settings_obj: object,
    strategy_set: RuntimeStrategySet | None,
) -> bool:
    return bool(
        run_loop_uses_target_delta(settings_obj)
        and (
            str(getattr(settings_obj, "MODE", "") or "").strip().lower() == "live"
            or str(getattr(settings_obj, "APPROVED_STRATEGY_PROFILE_PATH", "") or "").strip()
            or str(getattr(settings_obj, "STRATEGY_APPROVED_PROFILE_PATH", "") or "").strip()
            or (
                strategy_set is not None
                and str(strategy_set.source or "").strip() == "RUNTIME_STRATEGY_SET_JSON"
            )
        )
    )


def _allocation_context_fields(decision, *, runtime_pair: str) -> dict[str, object]:
    target = decision.target_for_pair(str(runtime_pair))
    target_payload = None if target is None else target.as_dict()
    decision_payload = decision.as_dict()
    target_conflict = {}
    if target_payload is not None:
        raw_conflict = target_payload.get("conflict_resolution")
        if isinstance(raw_conflict, dict):
            target_conflict = raw_conflict
    return {
        "portfolio_target_present": target is not None,
        "portfolio_target_authoritative": False if target is None else bool(target.authoritative),
        "portfolio_target_hash": "" if target is None else target.content_hash(),
        "allocation_decision_hash": decision.content_hash(),
        "allocator_config_hash": decision.allocator_config_hash,
        "strategy_contribution_hash": decision.strategy_contribution_hash,
        "allocator_policy": (
            ""
            if target is None
            else f"{target.allocator_policy_name}:{target.allocator_policy_version}"
        ),
        "allocator_reason": decision.reason,
        "allocation_conflict_count": int(decision.conflict_resolution.get("conflict_count") or 0),
        "allocation_primary_block_reason": decision.primary_block_reason,
        "allocation_selected_priority": target_conflict.get("selected_priority"),
        "allocation_selected_strategies": list(target_conflict.get("selected_strategies") or []),
        "allocation_selected_strategy_instance_ids": list(
            target_conflict.get("selected_strategy_instance_ids") or []
        ),
        "allocation_selected_signals": list(target_conflict.get("selected_signals") or []),
        "allocation_selected_signal": str(target_conflict.get("selected_signal") or ""),
        "allocation_contributions": [item.as_dict() for item in decision.contributions],
        "portfolio_target": target_payload,
        "portfolio_allocation_decision": decision_payload,
        "allocation_exposure_boundary_artifact_hash": str(
            decision_payload.get("exposure_boundary_artifact_hash") or ""
        ),
        "strategy_risk_decision_hash": str(target_conflict.get("strategy_risk_decision_hash") or ""),
        "strategy_risk_policy_hash": str(target_conflict.get("strategy_risk_policy_hash") or ""),
        "strategy_risk_input_hash": str(target_conflict.get("strategy_risk_input_hash") or ""),
        "strategy_risk_evidence_hash": str(target_conflict.get("strategy_risk_evidence_hash") or ""),
        "strategy_risk_status": str(target_conflict.get("strategy_risk_status") or ""),
        "strategy_risk_reason_code": str(target_conflict.get("strategy_risk_block_reason_code") or ""),
    }


def _runtime_strategy_set_context_fields(strategy_set: RuntimeStrategySet | None) -> dict[str, object]:
    if strategy_set is None:
        return {
            "runtime_strategy_set_present": False,
            "runtime_multi_strategy_enabled": False,
            "active_strategy_set": [],
        }
    return {
        "runtime_strategy_set_present": True,
        "runtime_multi_strategy_enabled": strategy_set.multi_strategy_enabled,
        "runtime_strategy_set_source": strategy_set.source,
        "runtime_strategy_set_manifest_hash": runtime_strategy_set_manifest_hash(strategy_set),
        "active_strategy_set": [item.as_dict() for item in strategy_set.active_strategies],
        "active_strategy_count": len(strategy_set.active_strategies),
    }


def _runtime_pair_for_planning(
    strategy_set: RuntimeStrategySet | None,
    *,
    settings_obj: object = settings,
) -> str:
    if strategy_set is not None and strategy_set.market_scope is not None:
        return str(strategy_set.market_scope.pair)
    return str(getattr(settings_obj, "PAIR"))


def _allocation_single_pair_invariant_error(decision, *, runtime_pair: str) -> str | None:
    targets = tuple(getattr(decision, "targets", ()) or ())
    if len(targets) != 1:
        return "single_pair_allocation_target_count_mismatch"
    target = targets[0]
    if str(getattr(target, "pair", "") or "") != str(runtime_pair):
        return "single_pair_allocation_target_pair_mismatch"
    recorded_hash = str(target.as_dict().get("final_portfolio_target_hash") or "")
    if recorded_hash != target.content_hash():
        return "single_pair_portfolio_target_hash_mismatch"
    return None


def _allocation_single_pair_invariant_context(decision, *, runtime_pair: str) -> dict[str, object]:
    targets = tuple(getattr(decision, "targets", ()) or ())
    return {
        **runtime_scope_contract(),
        "runtime_scope_mode": RUNTIME_SCOPE_MODE,
        "multi_pair_portfolio_supported": False,
        "multi_pair_portfolio_fail_closed_reason": MULTI_PAIR_RUNTIME_UNSUPPORTED_REASON,
        "allocation_target_count": len(targets),
        "allocation_target_pairs": [str(getattr(target, "pair", "") or "") for target in targets],
        "runtime_pair": str(runtime_pair),
        "single_pair_allocation_invariant_checked": True,
    }


def _gate_payload(raw_gate: object | None) -> dict[str, object]:
    if raw_gate is None:
        return {}
    if isinstance(raw_gate, dict):
        return dict(raw_gate)
    as_dict = getattr(raw_gate, "as_dict", None)
    if callable(as_dict):
        payload = as_dict()
        return dict(payload) if isinstance(payload, dict) else {}
    return {}


def _selected_performance_gate_scope(decision, *, runtime_pair: str, manifest_hash: str | None) -> dict[str, object]:
    targets = tuple(getattr(decision, "targets", ()) or ())
    target = targets[0] if len(targets) == 1 else None
    conflict = dict(getattr(target, "conflict_resolution", {}) or {}) if target is not None else {}
    selected_signal = str(conflict.get("selected_signal") or "").upper()
    selected_ids = [str(item) for item in (conflict.get("selected_strategy_instance_ids") or [])]
    selected_id_set = set(selected_ids)
    selected_contributions = [
        item
        for item in getattr(decision, "contributions", ())
        if str(getattr(item, "strategy_instance_id", "") or "") in selected_id_set
        and str(getattr(item, "signal_direction", "") or "").upper() == selected_signal
        and selected_signal in {"BUY", "SELL"}
        and str(getattr(item, "pair", "") or "") == str(runtime_pair)
    ]
    return {
        "schema_version": 1,
        "scope": "allocator_selected_strategy_contributions",
        "performance_gate_policy": "all_allocator_selected_buy_sell_contributions_must_pass",
        "selected_by_allocator": True,
        "selected_pair": str(runtime_pair),
        "selected_signal": selected_signal,
        "selected_strategy_instance_ids": [item.strategy_instance_id for item in selected_contributions],
        "selected_strategy_names": [item.strategy_name for item in selected_contributions],
        "selected_signal_strategy_instance_ids": selected_ids,
        "runtime_strategy_set_manifest_hash": manifest_hash,
    }


def _aggregate_selected_performance_gate(
    evaluator: Callable[..., object],
    conn,
    decision,
    *,
    runtime_pair: str,
    manifest_hash: str | None,
    settings_obj: object = settings,
) -> dict[str, object] | None:
    scope = _selected_performance_gate_scope(
        decision,
        runtime_pair=runtime_pair,
        manifest_hash=manifest_hash,
    )
    selected_ids = set(scope["selected_strategy_instance_ids"])
    selected = [
        item
        for item in getattr(decision, "contributions", ())
        if str(getattr(item, "strategy_instance_id", "") or "") in selected_ids
    ]
    if not selected:
        return {
            "enabled": True,
            "allowed": True,
            "blocked": False,
            "reason_code": "STRATEGY_PERFORMANCE_GATE_NOT_APPLICABLE",
            "reason": "no allocator-selected BUY/SELL contribution requires performance gate",
            "recommended_next_action": "none",
            "summary": {"sample_count": 0, "expectancy_per_trade": 0.0, "net_pnl": 0.0},
            "thresholds": {"policy": scope["performance_gate_policy"]},
            "performance_gate_scope": scope,
            "per_strategy_gate_results": [],
            "blocking_strategy_instance_ids": [],
        }
    results: list[dict[str, object]] = []
    blocking_ids: list[str] = []
    for contribution in selected:
        try:
            raw = evaluator(
                conn,
                strategy_instance_id=str(contribution.strategy_instance_id),
                strategy_name=str(contribution.strategy_name),
                pair=str(contribution.pair),
                runtime_strategy_set_manifest_hash=manifest_hash,
                settings_obj=settings_obj,
            )
        except TypeError:
            raw = evaluator(
                conn,
                strategy_name=str(contribution.strategy_name),
                pair=str(contribution.pair),
            )
        payload = _gate_payload(raw)
        filter_scope = dict(
            payload.get("filter_scope")
            or dict(payload.get("thresholds") or {}).get("filter_scope")
            or dict(payload.get("summary") or {}).get("filter_scope")
            or {}
        )
        item = {
            "strategy_instance_id": contribution.strategy_instance_id,
            "strategy_name": contribution.strategy_name,
            "pair": contribution.pair,
            "selected_signal": contribution.signal_direction,
            "gate": payload,
            "filter_scope": filter_scope,
            "strategy_instance_id_filter_applied": bool(
                filter_scope.get("strategy_instance_id_filter_applied")
            ),
            "allowed": bool(payload.get("allowed", True)),
            "reason_code": payload.get("reason_code"),
        }
        results.append(item)
        if bool(payload.get("enabled", True)) and not bool(payload.get("allowed", True)):
            blocking_ids.append(str(contribution.strategy_instance_id))
    allowed = not blocking_ids
    first_payload = dict(results[0].get("gate") or {}) if results else {}
    first_blocking_payload = next(
        (dict(item.get("gate") or {}) for item in results if str(item.get("strategy_instance_id")) in blocking_ids),
        first_payload,
    )
    reason_code = (
        "STRATEGY_PERFORMANCE_OK"
        if allowed
        else "STRATEGY_PERFORMANCE_BLOCKED:SELECTED_ALLOCATOR_CONTRIBUTION"
    )
    return {
        "enabled": bool(first_payload.get("enabled", True)),
        "allowed": allowed,
        "blocked": not allowed,
        "reason_code": reason_code,
        "reason": (
            "all allocator-selected BUY/SELL contributions passed performance gate"
            if allowed
            else str(first_blocking_payload.get("reason") or "allocator-selected contribution failed gate")
        ),
        "recommended_next_action": (
            "none"
            if allowed
            else str(first_blocking_payload.get("recommended_next_action") or "review strategy-report")
        ),
        "summary": dict(first_blocking_payload.get("summary") or first_payload.get("summary") or {}),
        "thresholds": {
            **dict(first_blocking_payload.get("thresholds") or first_payload.get("thresholds") or {}),
            "policy": scope["performance_gate_policy"],
        },
        "performance_gate_scope": {**scope, "blocking_strategy_instance_ids": blocking_ids},
        "performance_gate_policy": scope["performance_gate_policy"],
        "per_strategy_gate_results": results,
        "blocking_strategy_instance_ids": blocking_ids,
    }


def _performance_gate_context_fields(gate: dict[str, object] | None) -> dict[str, object]:
    if not gate:
        return {}
    return {
        "performance_gate_scope": dict(gate.get("performance_gate_scope") or {}),
        "performance_gate_policy": gate.get("performance_gate_policy"),
        "per_strategy_gate_results": list(gate.get("per_strategy_gate_results") or []),
        "blocking_strategy_instance_ids": list(gate.get("blocking_strategy_instance_ids") or []),
        "strategy_performance_gate": gate,
        "strategy_performance_gate_reason_code": gate.get("reason_code"),
        "strategy_performance_gate_reason": gate.get("reason"),
        "strategy_performance_gate_blocked": bool(gate.get("blocked")),
    }


def _runtime_result_bundle_context_fields(
    result_bundle: RuntimeStrategyDecisionResultBundle | None,
) -> dict[str, object]:
    if result_bundle is None:
        return {}
    result_metadata = []
    for result in result_bundle.results:
        context = getattr(result, "base_context", {})
        payload = dict(context) if isinstance(context, Mapping) else {}
        result_metadata.append(
            {
                "strategy_name": str(getattr(result.decision, "strategy_name", "")).strip().lower(),
                "strategy_instance_id": payload.get("strategy_instance_id"),
                "runtime_decision_request_hash": payload.get("runtime_decision_request_hash"),
                "strategy_parameters_hash": payload.get("strategy_parameters_hash"),
                "approved_profile_hash": payload.get("approved_profile_hash"),
                "plugin_contract_hash": payload.get("plugin_contract_hash"),
                "runtime_contract_hash": payload.get("runtime_contract_hash"),
                "through_ts_ms": payload.get("through_ts_ms"),
            }
        )
    return {
        "runtime_strategy_result_bundle": result_bundle.as_dict(),
        "runtime_strategy_result_bundle_hash": result_bundle.content_hash(),
        "runtime_decision_request_hashes": [
            str(item.get("runtime_decision_request_hash") or "") for item in result_metadata
        ],
        "runtime_strategy_instance_ids": [
            str(item.get("strategy_instance_id") or "") for item in result_metadata
        ],
        "runtime_approved_profile_hashes": [
            item.get("approved_profile_hash") for item in result_metadata
        ],
        "runtime_strategy_parameter_hashes": [
            str(item.get("strategy_parameters_hash") or "") for item in result_metadata
        ],
        "runtime_plugin_contract_hashes": [
            item.get("plugin_contract_hash") for item in result_metadata
        ],
        "runtime_contract_hashes": [
            item.get("runtime_contract_hash") for item in result_metadata
        ],
    }


def run_loop_uses_target_delta(settings_obj: object = settings) -> bool:
    return (
        str(getattr(settings_obj, "EXECUTION_ENGINE", "lot_native") or "lot_native").strip().lower()
        == "target_delta"
    )


def load_previous_target_exposure_for_run_loop(
    conn,
    *,
    settings_obj: object = settings,
    runtime_pair: str | None = None,
) -> float | None:
    if not run_loop_uses_target_delta(settings_obj):
        return None
    pair = str(runtime_pair or getattr(settings_obj, "PAIR"))
    previous_target_state = load_target_position_state(conn, pair=pair)
    if previous_target_state is None:
        return None
    return float(previous_target_state.target_exposure_krw)


def resolve_target_position_state_for_run_loop(
    conn,
    *,
    readiness_payload: dict[str, object],
    reference_price: float | None,
    raw_signal: str,
    updated_ts: int,
    settings_obj: object = settings,
    runtime_pair: str | None = None,
) -> dict[str, object]:
    if not run_loop_uses_target_delta(settings_obj):
        return {
            "previous_target_exposure_krw": None,
            "target_policy_metadata": {},
            "target_state": None,
        }
    pair = str(runtime_pair or getattr(settings_obj, "PAIR"))
    previous_target_state = load_target_position_state(conn, pair=pair)
    execution_order_rules = resolve_execution_order_rules(readiness_payload, market=pair)
    policy = resolve_startup_target_position_policy(
        existing_target_state=previous_target_state,
        readiness_payload=readiness_payload,
        order_rules=execution_order_rules.as_order_rules(),
        reference_price=reference_price,
        raw_signal=raw_signal,
    )
    metadata = policy.as_dict()
    if policy.policy_action in {
        TARGET_POLICY_INITIALIZE_FLAT_TARGET,
        TARGET_POLICY_ADOPT_EXISTING_BROKER_POSITION,
        TARGET_POLICY_INITIALIZE_TRUE_DUST_FLAT,
    }:
        startup_source_by_action = {
            TARGET_POLICY_INITIALIZE_FLAT_TARGET: STARTUP_TARGET_SOURCE_POLICY_INITIALIZATION,
            TARGET_POLICY_ADOPT_EXISTING_BROKER_POSITION: STARTUP_TARGET_SOURCE_BROKER_POSITION_ADOPTION,
            TARGET_POLICY_INITIALIZE_TRUE_DUST_FLAT: STARTUP_TARGET_SOURCE_TRUE_DUST_FLAT_INITIALIZATION,
        }
        metadata["actual_target_source"] = startup_source_by_action[policy.policy_action]
        metadata["target_state_update_intent"] = {
            "pair": pair,
            "target_exposure_krw": float(policy.target_exposure_krw or 0.0),
            "target_qty": float(policy.target_qty or 0.0),
            "last_signal": str(raw_signal or "HOLD").upper(),
            "last_decision_id": None,
            "last_reference_price": float(reference_price or 0.0),
            "updated_ts": int(updated_ts),
            "target_origin": policy.target_origin,
            "adoption_reason": policy.adoption_reason,
            "adopted_broker_qty": policy.adopted_broker_qty,
            "adopted_broker_exposure_krw": policy.adopted_broker_exposure_krw,
            "created_from_signal": policy.created_from_signal,
            "actual_target_source": startup_source_by_action[policy.policy_action],
        }
    previous_exposure = (
        float(policy.target_exposure_krw or 0.0)
        if "target_state_update_intent" in metadata
        else None if previous_target_state is None else float(previous_target_state.target_exposure_krw)
    )
    if policy.policy_action == TARGET_POLICY_USE_EXISTING_TARGET and previous_target_state is not None:
        metadata.setdefault("target_origin", str(previous_target_state.target_origin or ""))
    return {
        "previous_target_exposure_krw": previous_exposure,
        "target_policy_metadata": metadata,
        "target_state": previous_target_state,
    }


def prepare_strategy_decision_persistence_context(
    *,
    decision_context: dict[str, object],
    execution_decision_summary: object,
    readiness_payload: dict[str, object],
    target_policy_metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    """Attach execution summary fields needed by persistence/logging."""
    if not hasattr(execution_decision_summary, "as_dict"):
        raise TypeError("execution_decision_summary_missing_as_dict")
    execution_decision = execution_decision_summary.as_dict()
    context = dict(decision_context)
    context["execution_decision"] = execution_decision
    context["final_action"] = execution_decision["final_action"]
    context.setdefault("authoritative_execution_signal", context.get("final_signal") or context.get("signal") or "HOLD")
    context["submit_expected"] = execution_decision["submit_expected"]
    context["pre_submit_proof_status"] = execution_decision["pre_submit_proof_status"]
    context["execution_block_reason"] = execution_decision["block_reason"]
    context["residual_live_sell_mode"] = execution_decision.get("residual_live_sell_mode")
    context["residual_buy_sizing_mode"] = execution_decision.get("residual_buy_sizing_mode")
    for plan_key in ("target_submit_plan", "residual_submit_plan", "buy_submit_plan"):
        plan_payload = execution_decision.get(plan_key)
        if isinstance(plan_payload, dict) and str(plan_payload.get("submit_plan_hash") or "").strip():
            context["execution_submit_plan_hash"] = str(plan_payload["submit_plan_hash"])
            context["submit_plan_hash"] = str(plan_payload["submit_plan_hash"])
            context["submit_authority_mode"] = str(plan_payload.get("submit_authority_mode") or "")
            context["submit_authority_policy_hash"] = str(
                plan_payload.get("submit_authority_policy_hash") or ""
            )
            context["exposure_boundary_artifact_hash"] = str(
                plan_payload.get("exposure_boundary_artifact_hash") or ""
            )
            for risk_key in (
                "pre_submit_risk_required",
                "pre_submit_risk_decision_authority",
                "pre_submit_risk_decision_hash",
                "pre_submit_risk_policy_hash",
                "pre_submit_risk_input_hash",
                "pre_submit_risk_evidence_hash",
                "pre_submit_risk_status",
                "pre_submit_risk_reason_code",
                "pre_submit_risk_plan_hash",
                "pre_submit_risk_state_source",
            ):
                if risk_key in plan_payload:
                    context[risk_key] = plan_payload[risk_key]
            break
    target_shadow = execution_decision.get("target_shadow_decision")
    if isinstance(target_shadow, dict):
        for target_key, target_value in target_shadow.items():
            context[target_key] = target_value
    if isinstance(target_policy_metadata, dict):
        for target_key, target_value in target_policy_metadata.items():
            context.setdefault(target_key, target_value)
    for key in READINESS_CONTEXT_KEYS:
        if key in readiness_payload:
            context[key] = readiness_payload[key]
    return context


def _live_real_target_delta_performance_gate_applies(settings_obj: object = settings) -> bool:
    return bool(
        run_loop_uses_target_delta(settings_obj)
        and str(getattr(settings_obj, "MODE")).strip().lower() == "live"
        and bool(getattr(settings_obj, "LIVE_REAL_ORDER_ARMED"))
        and not bool(getattr(settings_obj, "LIVE_DRY_RUN"))
    )


def _live_real_order_submit_plan_required() -> bool:
    return bool(
        str(settings.MODE).strip().lower() == "live"
        and bool(settings.LIVE_REAL_ORDER_ARMED)
        and not bool(settings.LIVE_DRY_RUN)
    )


@dataclass(frozen=True)
class ExecutionPlanner:
    settings_obj: object = settings
    readiness_snapshot_builder: Callable[..., object] = compute_runtime_readiness_snapshot
    performance_gate_evaluator: Callable[..., object] = evaluate_strategy_performance_gate
    summary_builder: Callable[..., ExecutionDecisionSummary] = build_typed_execution_decision_summary
    target_state_resolver: Callable[..., dict[str, object]] = resolve_target_position_state_for_run_loop
    persistence_context_builder: Callable[..., dict[str, object]] = prepare_strategy_decision_persistence_context
    broker_provider: Callable[[], object | None] = _no_broker_provider
    strict_promotion_mode: bool = True
    read_only_planning: bool = False

    @property
    def result_cls(self) -> type[ExecutionPlanningResult]:
        return ExecutionPlanningResult

    @property
    def typed_summary_builder(self) -> Callable[..., ExecutionDecisionSummary]:
        return build_typed_execution_decision_summary

    @staticmethod
    def live_real_target_delta_performance_gate_applies() -> bool:
        return _live_real_target_delta_performance_gate_applies()

    def _strategy_risk_broker(self) -> object | None:
        if str(getattr(self.settings_obj, "MODE", "") or "").strip().lower() != "live":
            return None
        try:
            return self.broker_provider()
        except Exception:
            return None

    def fail_closed_context(
        self,
        *,
        decision_context: dict[str, object],
        reason_code: str,
        exc: Exception | None = None,
    ) -> ExecutionPlanningResult:
        return self._fail_closed_context(
            decision_context=decision_context,
            reason_code=reason_code,
            exc=exc,
        )

    def plan_envelope(
        self,
        conn,
        envelope: DecisionEnvelope,
        *,
        updated_ts: int,
    ) -> ExecutionPlanBundle:
        planning_input = ExecutionPlanningInput.from_envelope(envelope)
        planning = self._plan_typed_input(
            conn,
            planning_input=planning_input,
            updated_ts=int(updated_ts),
        )
        submit_plan = _with_h74_submit_plan_evidence(
            _primary_submit_plan(planning.execution_decision_summary),
            context=planning.context,
            readiness_payload=planning.readiness_payload,
        )
        execution_decision_summary = _summary_with_primary_submit_plan(
            planning.execution_decision_summary,
            submit_plan,
        )
        context = dict(planning.context)
        context["planner_subphase"] = "lock_intent_build"
        try:
            batch = _build_execution_plan_batch_for_runtime_pair(
                conn,
                context=context,
                submit_plan=submit_plan,
                updated_ts=int(updated_ts),
                read_only=bool(self.read_only_planning),
            )
        except Exception as exc:
            planning = self._fail_closed_context(
                decision_context=context,
                reason_code="execution_plan_batch_unavailable",
                exc=exc,
            )
            return ExecutionPlanBundle(
                summary=None,
                submit_plan=None,
                persistence_context=dict(planning.context),
                readiness_payload=planning.readiness_payload,
                target_policy_metadata=planning.target_policy_metadata,
                planning_error=planning.planning_error,
                status=_plan_status(planning),
                execution_plan_batch=None,
            )
        context.update(
            {
                "decision_authority_source": "DecisionEnvelope.strategy_decision",
                "decision_envelope_present": True,
                "execution_plan_bundle_present": True,
                "submit_plan_source": None if submit_plan is None else submit_plan.source,
                "submit_plan_authority": (
                    None
                    if submit_plan is None
                    else "derived_from_execution_plan_batch_pair_plan"
                ),
                "persistence_context_authoritative": 0,
                "non_authoritative_observability_payload": True,
                "execution_plan_batch_authority": "ExecutionPlanBatch",
                "execution_plan_batch_hash": batch.content_hash(),
                "execution_plan_batch_id": batch.batch_id,
                "execution_plan_batch_pair_count": len(batch.pair_plans),
                "pair_execution_plan_hash": batch.pair_plans[0].content_hash(),
                "pair_execution_plan_pair": batch.pair_plans[0].pair,
                "batch_lock_status": batch.pair_plans[0].lock_status,
            }
        )
        bundle = ExecutionPlanBundle(
            summary=execution_decision_summary,
            submit_plan=submit_plan,
            persistence_context=context,
            readiness_payload=planning.readiness_payload,
            target_policy_metadata=planning.target_policy_metadata,
            planning_error=planning.planning_error,
            status=_plan_status(planning),
            execution_plan_batch=batch,
        )
        context["execution_plan_bundle"] = bundle.as_dict()
        context["execution_plan_bundle_hash"] = bundle.content_hash()
        return bundle

    def plan_runtime_strategy_results(
        self,
        conn,
        result_bundle: RuntimeStrategyDecisionResultBundle,
        *,
        updated_ts: int,
    ) -> ExecutionPlanBundle:
        if result_bundle.strategy_set.multi_strategy_enabled:
            planning_input = ExecutionPlanningInput.from_runtime_result_bundle(result_bundle)
        else:
            envelope = DecisionEnvelope.from_runtime_result(result_bundle.results[0])
            planning_input = ExecutionPlanningInput.from_envelope(envelope)
        planning = self._plan_typed_input(
            conn,
            planning_input=planning_input,
            updated_ts=int(updated_ts),
            runtime_result_bundle=result_bundle,
        )
        submit_plan = _with_h74_submit_plan_evidence(
            _primary_submit_plan(planning.execution_decision_summary),
            context=planning.context,
            readiness_payload=planning.readiness_payload,
        )
        execution_decision_summary = _summary_with_primary_submit_plan(
            planning.execution_decision_summary,
            submit_plan,
        )
        context = dict(planning.context)
        context["planner_subphase"] = "lock_intent_build"
        try:
            batch = _build_execution_plan_batch_for_runtime_pair(
                conn,
                context=context,
                submit_plan=submit_plan,
                updated_ts=int(updated_ts),
            )
        except Exception as exc:
            planning = self._fail_closed_context(
                decision_context=context,
                reason_code="execution_plan_batch_unavailable",
                exc=exc,
            )
            return ExecutionPlanBundle(
                summary=None,
                submit_plan=None,
                persistence_context=dict(planning.context),
                readiness_payload=planning.readiness_payload,
                target_policy_metadata=planning.target_policy_metadata,
                planning_error=planning.planning_error,
                status=_plan_status(planning),
                execution_plan_batch=None,
            )
        context.update(
            {
                "decision_authority_source": "PortfolioAllocator.portfolio_target",
                "representative_strategy_decision_authority": "non_authoritative_observability_only",
                "decision_envelope_present": not result_bundle.strategy_set.multi_strategy_enabled,
                "execution_plan_bundle_present": True,
                "submit_plan_source": None if submit_plan is None else submit_plan.source,
                "submit_plan_authority": (
                    None
                    if submit_plan is None
                    else "derived_from_execution_plan_batch_pair_plan"
                ),
                "persistence_context_authoritative": 0,
                "non_authoritative_observability_payload": True,
                "execution_plan_batch_authority": "ExecutionPlanBatch",
                "execution_plan_batch_hash": batch.content_hash(),
                "execution_plan_batch_id": batch.batch_id,
                "execution_plan_batch_pair_count": len(batch.pair_plans),
                "pair_execution_plan_hash": batch.pair_plans[0].content_hash(),
                "pair_execution_plan_pair": batch.pair_plans[0].pair,
                "batch_lock_status": batch.pair_plans[0].lock_status,
            }
        )
        bundle = ExecutionPlanBundle(
            summary=execution_decision_summary,
            submit_plan=submit_plan,
            persistence_context=context,
            readiness_payload=planning.readiness_payload,
            target_policy_metadata=planning.target_policy_metadata,
            planning_error=planning.planning_error,
            status=_plan_status(planning),
            execution_plan_batch=batch,
        )
        context["execution_plan_bundle"] = bundle.as_dict()
        context["execution_plan_bundle_hash"] = bundle.content_hash()
        return bundle

    def _planning_context_from_envelope_input(
        self,
        planning_input: ExecutionPlanningInput,
    ) -> dict[str, object]:
        decision = planning_input.strategy_decision
        context = _thaw_mapping(planning_input.base_observability_context)
        replay_fingerprint = _thaw_mapping(planning_input.replay_fingerprint)
        boundary = _thaw_mapping(planning_input.boundary)
        context.update(
            {
                "ts": int(planning_input.candle_ts),
                "last_close": float(planning_input.market_price),
                "market_price": float(planning_input.market_price),
                "strategy": getattr(decision, "strategy_name", ""),
                "signal": getattr(decision, "final_signal", "HOLD"),
                "reason": getattr(decision, "final_reason", ""),
                "raw_signal": getattr(decision, "raw_signal", "HOLD"),
                "raw_reason": getattr(decision, "raw_reason", ""),
                "final_signal": getattr(decision, "final_signal", "HOLD"),
                "final_reason": getattr(decision, "final_reason", ""),
                "pure_policy_hash": getattr(decision, "policy_hash", ""),
                "policy_contract_hash": getattr(decision, "policy_contract_hash", ""),
                "policy_input_hash": getattr(decision, "policy_input_hash", ""),
                "policy_decision_hash": getattr(decision, "policy_decision_hash", ""),
                "pure_policy_trace": decision.as_trace() if hasattr(decision, "as_trace") else {},
                "replay_fingerprint": replay_fingerprint,
                "replay_fingerprint_hash": sha256_prefixed(replay_fingerprint),
                "boundary": boundary,
                "decision_authority_source": "DecisionEnvelope.strategy_decision",
                "decision_envelope_present": True,
                "persistence_context_authoritative": 0,
                "non_authoritative_observability_payload": True,
            }
        )
        context.update(dict(planning_input.policy_hashes))
        for key in (
            "decision_input_bundle_hash",
            "decision_input_contract_hash",
            "decision_input_bundle_payload_hash",
            "snapshot_projector_version",
            "snapshot_projector_hash",
            "market_snapshot_hash",
            "market_feature_hash",
            "canonical_feature_projection_hash",
            "final_exit_decision_input_hash",
            "position_snapshot_hash",
            "execution_constraints_hash",
            "policy_config_hash",
            "exit_policy_config_hash",
        ):
            if str(context.get(key) or "").strip():
                continue
            value = replay_fingerprint.get(key)
            if str(value or "").strip():
                context[key] = value
        execution_intent = getattr(decision, "execution_intent", None)
        if execution_intent is not None and hasattr(execution_intent, "as_dict"):
            context["strategy_trace"] = {
                **dict(context.get("strategy_trace") or {}),
                "execution_intent": execution_intent.as_dict(),
                "execution_intent_authority": "non_authoritative_strategy_hint",
            }
        return context

    def _plan_typed_input(
        self,
        conn,
        *,
        planning_input: ExecutionPlanningInput,
        updated_ts: int,
        runtime_result_bundle: RuntimeStrategyDecisionResultBundle | None = None,
    ) -> ExecutionPlanningResult:
        context = self._planning_context_from_envelope_input(planning_input)
        try:
            strategy_set = None if runtime_result_bundle is None else runtime_result_bundle.strategy_set
            runtime_pair = _runtime_pair_for_planning(strategy_set, settings_obj=self.settings_obj)
            context.update(_runtime_strategy_set_context_fields(strategy_set))
            context.update(_runtime_result_bundle_context_fields(runtime_result_bundle))
            context["runtime_pair"] = runtime_pair
            context["planner_subphase"] = "readiness_snapshot"
            readiness_payload = self.readiness_snapshot_builder(conn).as_dict()
            h74_authority_fields = _h74_authority_planning_fields(self.settings_obj)
            if h74_authority_fields:
                readiness_payload = {**readiness_payload, **h74_authority_fields}
                context.update(h74_authority_fields)
            strategy_performance_gate = None
            pre_allocation_target_resolution_applied = False
            reference_price = context.get("market_price", context.get("last_close", context.get("close")))
            if runtime_result_bundle is None:
                context["planner_subphase"] = "target_state_resolution"
                target_resolution = self.target_state_resolver(
                    conn,
                    readiness_payload=readiness_payload,
                    reference_price=reference_price,
                    raw_signal=planning_input.final_signal,
                    updated_ts=int(updated_ts),
                    settings_obj=self.settings_obj,
                    runtime_pair=runtime_pair,
                )
                previous_target_exposure_krw = target_resolution.get("previous_target_exposure_krw")
                target_policy_metadata = dict(target_resolution.get("target_policy_metadata", {}))
            else:
                try:
                    context["planner_subphase"] = "target_state_resolution"
                    previous_target_exposure_krw = load_previous_target_exposure_for_run_loop(
                        conn,
                        settings_obj=self.settings_obj,
                        runtime_pair=runtime_pair,
                    )
                except AttributeError:
                    previous_target_exposure_krw = None
                target_policy_metadata = {}
                if (
                    runtime_result_bundle is not None
                    and len(runtime_result_bundle.results) == 1
                    and str(planning_input.final_signal or "").upper() in {"BUY", "SELL", "HOLD"}
                ):
                    context["planner_subphase"] = "target_state_resolution"
                    target_resolution = self.target_state_resolver(
                        conn,
                        readiness_payload=readiness_payload,
                        reference_price=reference_price,
                        raw_signal=planning_input.final_signal,
                        updated_ts=int(updated_ts),
                        settings_obj=self.settings_obj,
                        runtime_pair=runtime_pair,
                    )
                    previous_target_exposure_krw = target_resolution.get("previous_target_exposure_krw")
                    target_policy_metadata = dict(target_resolution.get("target_policy_metadata", {}))
                    context.update(target_policy_metadata)
                    pre_allocation_target_resolution_applied = True
            fallback_target_exposure, fallback_target_source, fallback_legacy = (
                _allocator_target_exposure_authority(self.settings_obj)
            )
            strict_target_exposure = _strict_target_exposure_required(
                settings_obj=self.settings_obj,
                strategy_set=strategy_set,
            )
            allocation_config = PortfolioAllocatorConfig(
                target_exposure_krw=fallback_target_exposure,
                target_exposure_source=(
                    "runtime_strategy_spec.desired_exposure_krw"
                    if strict_target_exposure
                    else f"paper_legacy_compat:{fallback_target_source}"
                ),
                require_explicit_strategy_target_exposure=strict_target_exposure,
                strategy_priorities=(
                    {}
                    if strategy_set is None
                    else {derive_strategy_instance_id(item): item.priority for item in strategy_set.active_strategies}
                ),
                strategy_weights=(
                    {}
                    if strategy_set is None
                    else {derive_strategy_instance_id(item): item.weight for item in strategy_set.active_strategies}
                ),
            )
            context.update(
                {
                    "target_exposure_source": allocation_config.target_exposure_source,
                    "allocation_target_source": allocation_config.target_exposure_source,
                    "strict_target_exposure_required": strict_target_exposure,
                    "legacy_target_exposure_fallback_used": bool(
                        fallback_legacy and not strict_target_exposure
                    ),
                }
            )
            previous_target_exposure_by_pair = {runtime_pair: previous_target_exposure_krw}
            reference_price_by_pair = {
                runtime_pair: None if reference_price is None else float(reference_price)
            }
            context.update(
                {
                    "previous_target_exposure_by_pair": dict(previous_target_exposure_by_pair),
                    "reference_price_by_pair": dict(reference_price_by_pair),
                    "pair_aware_allocation_input": True,
                    "single_pair_scalar_pair_map_equivalence": True,
                    "previous_target_exposure_lookup_source": "target_position_state(pair)",
                    "reference_price_lookup_source": "runtime_market_price",
                }
            )
            if runtime_result_bundle is None:
                strategy_preference = strategy_decision_to_preference(
                    planning_input.strategy_decision,
                    pair=runtime_pair,
                    strategy_instance_id=str(
                        context.get("strategy_instance_id")
                        or planning_input.strategy_decision.strategy_name
                    ),
                    desired_exposure_krw=(
                        None if strict_target_exposure else fallback_target_exposure
                    ),
                    metadata={
                        "target_exposure_source": allocation_config.target_exposure_source,
                        "allocation_target_source": allocation_config.target_exposure_source,
                        "legacy_compatibility_used": bool(
                            fallback_legacy and not strict_target_exposure
                        ),
                    },
                )
                preferences = (strategy_preference,)
            else:
                preference_list = []
                runtime_result_contexts = []
                for result in runtime_result_bundle.results:
                    result_context = getattr(result, "base_context", {})
                    result_instance_id = (
                        str(result_context.get("strategy_instance_id") or "").strip()
                        if isinstance(result_context, Mapping)
                        else ""
                    )
                    spec = (
                        runtime_result_bundle.strategy_set.spec_for_instance(result_instance_id)
                        if result_instance_id
                        else runtime_result_bundle.strategy_set.spec_for_strategy(result.decision.strategy_name)
                    )
                    if spec is None:
                        raise ValueError(
                            f"runtime_strategy_spec_missing:{result.decision.strategy_name}"
                        )
                    strategy_instance_id = derive_strategy_instance_id(spec)
                    result_metadata = {
                        "strategy_instance_id": strategy_instance_id,
                        "runtime_strategy_priority": spec.priority,
                        "runtime_strategy_set_source": runtime_result_bundle.strategy_set.source,
                    }
                    if isinstance(result_context, Mapping):
                        result_metadata.update(
                            {
                                key: result_context.get(key)
                                for key in (
                                    "strategy_parameters",
                                    "strategy_parameters_raw",
                                    "strategy_parameters_materialized",
                                    "strategy_parameters_hash",
                                    "approved_profile_path",
                                    "approved_profile_hash",
                                    "runtime_contract_hash",
                                    "plugin_contract_hash",
                                    "runtime_decision_request_hash",
                                    "runtime_scope_key",
                                    "scope_key_hash",
                                    "parameter_source",
                                )
                                if key in result_context
                            }
                        )
                    scope_key_hash = str(result_metadata.get("scope_key_hash") or "").strip()
                    runtime_contract_hash = str(
                        result_metadata.get("runtime_contract_hash") or ""
                    ).strip()
                    virtual_lifecycle_artifact: dict[str, object]
                    missing_virtual_lifecycle_fields = [
                        field
                        for field, value in (
                            ("scope_key_hash", scope_key_hash),
                            ("runtime_contract_hash", runtime_contract_hash),
                        )
                        if not str(value or "").strip()
                    ]
                    if missing_virtual_lifecycle_fields:
                        skip_reason = "strategy_virtual_lifecycle_missing:" + ",".join(
                            sorted(missing_virtual_lifecycle_fields)
                        )
                        live_like_virtual_lifecycle = str(
                            getattr(self.settings_obj, "MODE", "") or ""
                        ).strip().lower() == "live" or bool(
                            getattr(self.settings_obj, "LIVE_REAL_ORDER_ARMED", False)
                        )
                        if live_like_virtual_lifecycle:
                            raise ValueError(skip_reason)
                        virtual_lifecycle_artifact = build_strategy_virtual_lifecycle_skipped_artifact(
                            strategy_instance_id=strategy_instance_id,
                            strategy_name=spec.strategy_name,
                            pair=str(spec.pair),
                            interval=str(spec.interval),
                            scope_key_hash=scope_key_hash,
                            runtime_contract_hash=runtime_contract_hash,
                            skip_reason=skip_reason,
                        )
                        result_metadata.update(
                            {
                                "virtual_target_lifecycle_authority": virtual_lifecycle_artifact[
                                    "authority"
                                ],
                                "virtual_target_lifecycle_status": virtual_lifecycle_artifact[
                                    "virtual_target_lifecycle_status"
                                ],
                                "virtual_target_lifecycle_skip_reason": skip_reason,
                                "virtual_target_live_submit_authority": False,
                                "virtual_target_state_before_hash": "",
                                "virtual_target_state_after_hash": "",
                                "virtual_target_state_evidence_hash": virtual_lifecycle_artifact[
                                    "evidence_hash"
                                ],
                                "virtual_target_lifecycle_transition_hash": virtual_lifecycle_artifact[
                                    "transition_hash"
                                ],
                                "virtual_target_lifecycle_transition_artifact": virtual_lifecycle_artifact,
                            }
                        )
                    elif not callable(getattr(conn, "execute", None)):
                        skip_reason = "strategy_virtual_lifecycle_missing:sqlite_persistence_connection"
                        virtual_lifecycle_artifact = build_strategy_virtual_lifecycle_skipped_artifact(
                            strategy_instance_id=strategy_instance_id,
                            strategy_name=spec.strategy_name,
                            pair=str(spec.pair),
                            interval=str(spec.interval),
                            scope_key_hash=scope_key_hash,
                            runtime_contract_hash=runtime_contract_hash,
                            skip_reason=skip_reason,
                        )
                        result_metadata.update(
                            {
                                "virtual_target_lifecycle_authority": virtual_lifecycle_artifact[
                                    "authority"
                                ],
                                "virtual_target_lifecycle_status": virtual_lifecycle_artifact[
                                    "virtual_target_lifecycle_status"
                                ],
                                "virtual_target_lifecycle_skip_reason": skip_reason,
                                "virtual_target_live_submit_authority": False,
                                "virtual_target_state_before_hash": "",
                                "virtual_target_state_after_hash": "",
                                "virtual_target_state_evidence_hash": virtual_lifecycle_artifact[
                                    "evidence_hash"
                                ],
                                "virtual_target_lifecycle_transition_hash": virtual_lifecycle_artifact[
                                    "transition_hash"
                                ],
                                "virtual_target_lifecycle_transition_artifact": virtual_lifecycle_artifact,
                            }
                        )
                    else:
                        context["planner_subphase"] = "virtual_target_state_load"
                        previous_virtual_state = load_strategy_virtual_target_state(
                            conn,
                            strategy_instance_id=strategy_instance_id,
                            pair=str(spec.pair),
                            interval=str(spec.interval),
                            scope_key_hash=scope_key_hash,
                        )
                        virtual_target_exposure = spec.desired_exposure_krw
                        if virtual_target_exposure is None and not strict_target_exposure:
                            virtual_target_exposure = fallback_target_exposure
                        virtual_state = evolve_strategy_virtual_target_state(
                            previous=previous_virtual_state,
                            strategy_instance_id=strategy_instance_id,
                            strategy_name=spec.strategy_name,
                            pair=str(spec.pair),
                            interval=str(spec.interval),
                            scope_key_hash=scope_key_hash,
                            runtime_contract_hash=runtime_contract_hash,
                            signal=str(result.decision.final_signal or "HOLD"),
                            target_exposure_krw=virtual_target_exposure,
                            reference_price=(
                                None if reference_price is None else float(reference_price)
                            ),
                            updated_ts=int(updated_ts),
                            evidence={
                                "runtime_strategy_decision_request_hash": result_metadata.get(
                                    "runtime_decision_request_hash"
                                ),
                                "policy_input_hash": getattr(
                                    result.decision,
                                    "policy_input_hash",
                                    "",
                                ),
                                "policy_decision_hash": getattr(
                                    result.decision,
                                    "policy_decision_hash",
                                    "",
                                ),
                                "strategy_instance_id": strategy_instance_id,
                                "pair": str(spec.pair),
                                "interval": str(spec.interval),
                                "non_authoritative": True,
                                "live_submit_authority": False,
                            },
                        )
                        before_hash = (
                            ""
                            if previous_virtual_state is None
                            else previous_virtual_state.content_hash()
                        )
                        after_payload = virtual_state.as_dict()
                        virtual_lifecycle_artifact = build_strategy_virtual_lifecycle_transition_artifact(
                            strategy_instance_id=strategy_instance_id,
                            strategy_name=spec.strategy_name,
                            pair=str(spec.pair),
                            interval=str(spec.interval),
                            scope_key_hash=scope_key_hash,
                            runtime_contract_hash=runtime_contract_hash,
                            before_hash=before_hash,
                            after_hash=virtual_state.content_hash(),
                            evidence_hash=virtual_state.evidence_hash,
                        )
                        result_metadata.update(
                            {
                                "virtual_target_lifecycle_authority": (
                                    virtual_lifecycle_artifact["authority"]
                                ),
                                "virtual_target_lifecycle_status": virtual_lifecycle_artifact[
                                    "virtual_target_lifecycle_status"
                                ],
                                "virtual_target_live_submit_authority": False,
                                "virtual_target_state_before_hash": before_hash,
                                "virtual_target_state_after_hash": virtual_state.content_hash(),
                                "virtual_target_state_evidence_hash": virtual_state.evidence_hash,
                                "virtual_target_lifecycle_transition_hash": virtual_lifecycle_artifact[
                                    "transition_hash"
                                ],
                                "virtual_target_lifecycle_transition_artifact": virtual_lifecycle_artifact,
                                "virtual_target_state_artifact": after_payload,
                                "virtual_target_state_update_intent": after_payload,
                            }
                        )
                    runtime_result_contexts.append(result_metadata)
                    strategy_risk_profile_payload = None
                    strategy_risk_decision_payload = None
                    live_like_for_risk = (
                        str(getattr(self.settings_obj, "MODE", "") or "").strip().lower() == "live"
                    )
                    risk_profile = None
                    if live_like_for_risk or spec.risk_policy is not None or spec.approved_profile_path:
                        try:
                            authority_context = ProfileAuthorityContext.for_strategy_set(
                                runtime_result_bundle.strategy_set,
                                settings_obj=self.settings_obj,
                            )
                            materialized_instance = RuntimeDecisionRequestBuilder(
                                settings_obj=self.settings_obj
                            ).with_authority_context(authority_context).materialize_instance(spec)
                            risk_profile = materialized_instance.risk_profile
                        except Exception:
                            if live_like_for_risk:
                                raise
                            risk_profile = None
                    if risk_profile is None and not live_like_for_risk:
                        risk_profile = strategy_risk_profile_from_profile_payload(
                            strategy_instance_id=strategy_instance_id,
                            strategy_name=spec.strategy_name,
                            pair=str(spec.pair),
                            interval=str(spec.interval),
                            profile_payload=None,
                            approved_runtime_profile_path=None,
                            approved_runtime_profile_hash=None,
                            inline_risk_policy=spec.risk_policy,
                            declared_risk_policy_hash=spec.risk_policy_hash,
                            live_like=False,
                            live_real_order=False,
                        )
                    if risk_profile is not None:
                        enforced = risk_profile.enforcement_mode == "enforced"
                        context["planner_subphase"] = "strategy_risk_snapshot"
                        if risk_profile.policy.policy_status == "disabled_explicit":
                            from .risk_contract import RiskSnapshot

                            snapshot = RiskSnapshot(
                                evaluation_ts_ms=int(result.candle_ts),
                                mark_price=float(result.market_price),
                                state_source="risk_policy_disabled_explicit",
                                evidence={
                                    "strategy_instance_id": strategy_instance_id,
                                    "strategy_name": spec.strategy_name,
                                    "pair": str(spec.pair),
                                    "interval": str(spec.interval),
                                    "policy_status": "disabled_explicit",
                                    "missing_policy": risk_profile.policy.missing_policy,
                                    "risk_enforcement_mode": risk_profile.enforcement_mode,
                                    "risk_profile_source": risk_profile.risk_profile_source,
                                },
                            )
                            missing_state = ()
                        elif not any(
                            (
                                float(risk_profile.policy.max_daily_loss_krw) > 0.0,
                                float(risk_profile.policy.max_position_loss_pct) > 0.0,
                                int(risk_profile.policy.max_daily_order_count) > 0,
                                int(risk_profile.policy.max_trade_count_per_day) > 0,
                                float(risk_profile.policy.max_drawdown_pct) > 0.0,
                                int(risk_profile.policy.cooldown_after_loss_min) > 0,
                                bool(risk_profile.policy.kill_switch),
                            )
                        ):
                            from .risk_contract import RiskSnapshot

                            snapshot = RiskSnapshot(
                                evaluation_ts_ms=int(result.candle_ts),
                                mark_price=float(result.market_price),
                                state_source="risk_policy_no_runtime_state_required",
                                evidence={
                                    "strategy_instance_id": strategy_instance_id,
                                    "strategy_name": spec.strategy_name,
                                    "pair": str(spec.pair),
                                    "interval": str(spec.interval),
                                    "scope": "strategy_instance",
                                    "state_derivation": "not_required_no_active_strategy_limits",
                                },
                            )
                            missing_state = ()
                        else:
                            snapshot = StrategyRiskStateProvider(
                                conn,
                                max_open_order_age_sec=int(
                                    getattr(self.settings_obj, "MAX_OPEN_ORDER_AGE_SEC", 300) or 300
                                ),
                            ).snapshot(
                                strategy_instance_id=strategy_instance_id,
                                strategy_name=spec.strategy_name,
                                pair=str(spec.pair),
                                interval=str(spec.interval),
                                as_of_ts_ms=int(result.candle_ts),
                                mark_price=float(result.market_price),
                                policy=risk_profile.policy,
                                broker=self._strategy_risk_broker(),
                                enforced=enforced,
                            )
                            missing_state = missing_required_risk_state(risk_profile.policy, snapshot)
                        if enforced and missing_state:
                            from .risk_contract import build_risk_decision

                            strategy_risk_decision_payload = build_risk_decision(
                                evaluation_point="pre_decision",
                                status="BLOCK",
                                reason_code="STRATEGY_RISK_STATE_INCOMPLETE",
                                reason="required runtime risk state is unavailable",
                                allowed_actions=("HOLD",),
                                recommended_action="halt",
                                snapshot=snapshot,
                                policy=risk_profile.policy,
                                evidence={
                                    **dict(snapshot.evidence),
                                    "missing_required_risk_state": list(missing_state),
                                },
                            ).as_dict()
                        else:
                            strategy_risk_decision_payload = RiskPolicyEngine(
                                risk_profile.policy
                            ).evaluate_pre_decision(snapshot).as_dict()
                        strategy_risk_profile_payload = {
                            **risk_profile.as_dict(),
                            "strategy_risk_profile_hash": risk_profile.profile_hash(),
                        }
                        result_metadata.update(
                            {
                                "strategy_risk_profile_hash": risk_profile.profile_hash(),
                                "strategy_risk_policy_hash": risk_profile.risk_policy_hash,
                                "strategy_risk_decision_hash": strategy_risk_decision_payload.get(
                                    "risk_decision_hash"
                                )
                                if isinstance(strategy_risk_decision_payload, dict)
                                else None,
                                "strategy_risk_input_hash": strategy_risk_decision_payload.get(
                                    "risk_input_hash"
                                )
                                if isinstance(strategy_risk_decision_payload, dict)
                                else None,
                                "strategy_risk_evidence_hash": strategy_risk_decision_payload.get(
                                    "risk_evidence_hash"
                                )
                                if isinstance(strategy_risk_decision_payload, dict)
                                else None,
                                "strategy_risk_status": strategy_risk_decision_payload.get("status")
                                if isinstance(strategy_risk_decision_payload, dict)
                                else None,
                                "strategy_risk_reason_code": strategy_risk_decision_payload.get(
                                    "reason_code"
                                )
                                if isinstance(strategy_risk_decision_payload, dict)
                                else None,
                                "strategy_risk_state_source": strategy_risk_decision_payload.get(
                                    "state_source"
                                )
                                if isinstance(strategy_risk_decision_payload, dict)
                                else None,
                            }
                        )
                    preference_list.append(
                        strategy_decision_to_preference(
                            result.decision,
                            pair=str(spec.pair),
                            strategy_instance_id=strategy_instance_id,
                            desired_exposure_krw=spec.desired_exposure_krw,
                            desired_weight=spec.weight,
                            risk_budget_krw=spec.risk_budget_krw,
                            max_target_exposure_krw=spec.max_target_exposure_krw,
                            risk_policy=spec.risk_policy,
                            risk_snapshot=spec.risk_snapshot,
                            strategy_risk_profile=strategy_risk_profile_payload,
                            strategy_risk_decision=strategy_risk_decision_payload,
                            virtual_lifecycle_evidence=(
                                result_metadata.get("virtual_target_lifecycle_transition_artifact")
                                if isinstance(
                                    result_metadata.get(
                                        "virtual_target_lifecycle_transition_artifact"
                                    ),
                                    Mapping,
                                )
                                else None
                            ),
                            metadata=result_metadata,
                        )
                    )
                context["runtime_strategy_result_contexts"] = runtime_result_contexts
                context["strategy_virtual_lifecycle_transition_hashes"] = [
                    str(item.get("virtual_target_lifecycle_transition_hash") or "")
                    for item in runtime_result_contexts
                    if str(item.get("virtual_target_lifecycle_transition_hash") or "").strip()
                ]
                context["strategy_virtual_lifecycle_transition_artifacts"] = [
                    dict(item.get("virtual_target_lifecycle_transition_artifact") or {})
                    for item in runtime_result_contexts
                    if isinstance(item.get("virtual_target_lifecycle_transition_artifact"), Mapping)
                ]
                context["virtual_target_state_update_intents"] = [
                    dict(item.get("virtual_target_state_update_intent") or {})
                    for item in runtime_result_contexts
                    if isinstance(item.get("virtual_target_state_update_intent"), Mapping)
                ]
                preferences = tuple(preference_list)
            context["strategy_preference_count"] = len(preferences)
            context["strategy_preferences"] = [item.as_dict() for item in preferences]
            if len(preferences) == 1:
                preference_payload = preferences[0].as_dict()
                strategy_risk_decision = preference_payload.get("strategy_risk_decision")
                if isinstance(strategy_risk_decision, Mapping):
                    context["strategy_risk_decision"] = dict(strategy_risk_decision)
                    context["strategy_risk_decision_hash"] = strategy_risk_decision.get(
                        "risk_decision_hash"
                    )
                    context["strategy_risk_policy_hash"] = strategy_risk_decision.get(
                        "risk_policy_hash"
                    )
                    context["strategy_risk_input_hash"] = strategy_risk_decision.get(
                        "risk_input_hash"
                    )
                    context["strategy_risk_evidence_hash"] = strategy_risk_decision.get(
                        "risk_evidence_hash"
                    )
                    context["strategy_risk_status"] = strategy_risk_decision.get("status")
                    context["strategy_risk_reason_code"] = strategy_risk_decision.get(
                        "reason_code"
                    )
                    context["strategy_risk_state_source"] = strategy_risk_decision.get(
                        "state_source"
                    )
            preference_set = SignalAggregator().aggregate(preferences)
            allocation_input = PortfolioAllocationInput(
                preference_set=preference_set,
                allocator_config=allocation_config,
                previous_target_exposure_krw=(
                    None
                    if previous_target_exposure_krw is None
                    else float(previous_target_exposure_krw)
                ),
                reference_price=(
                    None if reference_price is None else float(reference_price)
                ),
                previous_target_exposure_by_pair=previous_target_exposure_by_pair,
                reference_price_by_pair=reference_price_by_pair,
            )
            context["portfolio_allocation_input"] = allocation_input.as_dict()
            context["allocation_input_hash"] = allocation_input.content_hash()
            allocation_decision = PortfolioAllocator(allocation_config).allocate(allocation_input)
            allocation_context = _allocation_context_fields(allocation_decision, runtime_pair=runtime_pair)
            context.update(allocation_context)
            context.update(
                _allocation_single_pair_invariant_context(
                    allocation_decision,
                    runtime_pair=runtime_pair,
                )
            )
            invariant_error = _allocation_single_pair_invariant_error(
                allocation_decision,
                runtime_pair=runtime_pair,
            )
            if invariant_error is not None:
                return self._fail_closed_context(
                    decision_context=context,
                    reason_code=invariant_error,
                )
            portfolio_target = allocation_decision.target_for_pair(str(runtime_pair))
            selected_signal = str(context.get("allocation_selected_signal") or "").upper()
            target_authoritative = bool(portfolio_target is not None and portfolio_target.authoritative)
            allocation_authoritative = bool(allocation_decision.authoritative and target_authoritative)
            if _live_real_target_delta_performance_gate_applies(self.settings_obj) and allocation_authoritative:
                strategy_performance_gate = _aggregate_selected_performance_gate(
                    self.performance_gate_evaluator,
                    conn,
                    allocation_decision,
                    runtime_pair=runtime_pair,
                    manifest_hash=(
                        str(context.get("runtime_strategy_set_manifest_hash") or "")
                        if context.get("runtime_strategy_set_manifest_hash")
                        else None
                    ),
                    settings_obj=self.settings_obj,
                )
                context.update(_performance_gate_context_fields(strategy_performance_gate))
                if bool(strategy_performance_gate.get("blocked")):
                    return self._fail_closed_context(
                        decision_context=context,
                        reason_code="selected_strategy_performance_gate_blocked",
                    )
            if (
                runtime_result_bundle is not None
                and allocation_authoritative
                and selected_signal in {"BUY", "SELL", "HOLD"}
            ):
                authoritative_signal = selected_signal
                authoritative_reason = str(context.get("allocator_reason") or "allocated")
            elif runtime_result_bundle is not None:
                authoritative_signal = "HOLD"
                authoritative_reason = str(
                    context.get("allocation_primary_block_reason")
                    or context.get("allocator_reason")
                    or "portfolio_allocation_not_authoritative"
                )
                context["portfolio_target_authoritative"] = False
                context["submit_expected"] = False
                context["allocation_selected_signal"] = ""
            else:
                authoritative_signal = planning_input.final_signal
                authoritative_reason = planning_input.final_reason
            if runtime_result_bundle is not None:
                if (
                    pre_allocation_target_resolution_applied
                    and authoritative_signal == planning_input.final_signal
                ):
                    resolved_previous_target_exposure = previous_target_exposure_krw
                else:
                    context["planner_subphase"] = "target_state_resolution"
                    target_resolution = self.target_state_resolver(
                        conn,
                        readiness_payload=readiness_payload,
                        reference_price=reference_price,
                        raw_signal=authoritative_signal,
                        updated_ts=int(updated_ts),
                        settings_obj=self.settings_obj,
                        runtime_pair=runtime_pair,
                    )
                    resolved_previous_target_exposure = target_resolution.get("previous_target_exposure_krw")
                    if previous_target_exposure_krw is None:
                        previous_target_exposure_krw = resolved_previous_target_exposure
                    target_policy_metadata = dict(target_resolution.get("target_policy_metadata", {}))
                    context.update(target_policy_metadata)
            if runtime_result_bundle is not None and authoritative_signal in {"BUY", "SELL", "HOLD"}:
                planning_input = replace(
                    planning_input,
                    strategy_decision=replace(
                        planning_input.strategy_decision,
                        raw_signal=authoritative_signal,
                        final_signal=authoritative_signal,
                        final_reason=authoritative_reason,
                    ),
                )
                context["signal"] = authoritative_signal
                context["raw_signal"] = authoritative_signal
                context["final_signal"] = authoritative_signal
                context["final_reason"] = authoritative_reason
                context["authoritative_execution_signal"] = authoritative_signal
            else:
                context["authoritative_execution_signal"] = planning_input.final_signal
            readiness_payload = {**readiness_payload, **target_policy_metadata}
            if h74_authority_fields and authoritative_signal == "BUY":
                readiness_payload = _inject_h74_startup_gate(
                    readiness_payload=readiness_payload,
                    target_state={
                        "target_exposure_krw": previous_target_exposure_krw,
                    },
                    authority_fields=h74_authority_fields,
                )
                readiness_payload.update(
                    _h74_entry_cycle_fields(
                        planning_context={**context, **readiness_payload},
                        updated_ts=updated_ts,
                    )
                )
            if h74_authority_fields and authoritative_signal == "SELL":
                readiness_payload = _inject_h74_cycle_inventory(
                    conn,
                    readiness_payload=readiness_payload,
                    planning_context=context,
                )
            context.update(
                {
                    key: readiness_payload[key]
                    for key in (
                        "position_mode",
                        "hold_policy",
                        "authority_hash",
                        "authority_parameter_hash",
                        "source_artifact_hash",
                        "strategy_instance_id",
                        "cycle_id",
                        "h74_cycle_id",
                        "remaining_cycle_qty",
                        "h74_remaining_cycle_qty",
                        "locked_exit_qty",
                        "h74_cycle_inventory_error",
                        "h74_open_cycle_count",
                        "partial_fill_policy",
                        "h74_startup_gate_status",
                        "h74_startup_gate_reason_code",
                        "startup_gate_hash",
                        "contract_hash",
                    )
                    if key in readiness_payload
                }
            )
            context["planner_subphase"] = "execution_plan_batch_build"
            authority = ExecutionAuthorityEnvelope(
                planning_input=planning_input,
                readiness=ExecutionReadinessPlanningInput.from_payload(
                    readiness_payload,
                    target_policy_metadata=target_policy_metadata,
                ),
                target=ExecutionTargetPlanningInput(
                    previous_target_exposure_krw=(
                        None
                        if previous_target_exposure_krw is None
                        else float(previous_target_exposure_krw)
                    ),
                    portfolio_target=portfolio_target,
                    portfolio_target_hash="" if portfolio_target is None else portfolio_target.content_hash(),
                    allocation_decision_hash=allocation_decision.content_hash(),
                    allocator_config_hash=allocation_decision.allocator_config_hash,
                    strategy_contribution_hash=allocation_decision.strategy_contribution_hash,
                ),
                target_policy_metadata=target_policy_metadata,
                performance_gate_result=strategy_performance_gate,
                observability_context=context,
            )
            return self._plan_authority_envelope(
                authority=authority,
                readiness_payload=readiness_payload,
            )
        except Exception as exc:
            return self._fail_closed_context(
                decision_context=context,
                reason_code="execution_decision_unavailable",
                exc=exc,
            )

    def _plan_authority_envelope(
        self,
        *,
        authority: ExecutionAuthorityEnvelope,
        readiness_payload: dict[str, object],
    ) -> ExecutionPlanningResult:
        from .execution_service import build_execution_decision_summary

        context = dict(authority.observability_context)
        typed_builder = (
            build_typed_execution_decision_summary
            if self.summary_builder is build_execution_decision_summary
            else self.summary_builder
        )
        execution_decision_summary = typed_builder(
            typed_input=authority.typed_planning_input(),
            strategy_performance_gate=authority.performance_gate_result,
        )
        if run_loop_uses_target_delta(self.settings_obj):
            submit_plan = execution_decision_summary.typed_target_submit_plan()
            if (
                submit_plan is not None
                and bool(submit_plan.submit_expected)
                and str(submit_plan.side or "").upper() in {"BUY", "SELL"}
            ):
                context["authoritative_execution_signal"] = str(submit_plan.side).upper()
        context = self.persistence_context_builder(
            decision_context=context,
            execution_decision_summary=execution_decision_summary,
            readiness_payload=readiness_payload,
            target_policy_metadata=dict(authority.target_policy_metadata),
        )
        execution_decision = dict(context["execution_decision"])  # type: ignore[arg-type]
        return ExecutionPlanningResult(
            context=context,
            execution_decision=execution_decision,
            execution_decision_summary=execution_decision_summary,
            readiness_payload=readiness_payload,
            target_policy_metadata=dict(authority.target_policy_metadata),
        )

    def _fail_closed_context(
        self,
        *,
        decision_context: dict[str, object],
        reason_code: str,
        exc: Exception | None = None,
    ) -> ExecutionPlanningResult:
        context = dict(decision_context)
        execution_decision: dict[str, object] = {}
        context["execution_decision"] = execution_decision
        context["final_action"] = "BLOCK_RECOVERY"
        context["submit_expected"] = False
        context["pre_submit_proof_status"] = "failed"
        context["execution_block_reason"] = reason_code
        context["execution_decision_authoritative"] = 0
        context["persistence_context_authoritative"] = 0
        context["legacy_context_planning_used"] = False
        context["compatibility_fallback"] = False
        context["promotion_grade"] = False
        context["recommended_next_action"] = (
            "regenerate_decision_with_typed_execution_authority"
            if reason_code == "legacy_context_planning_disabled"
            else "inspect_execution_planning_failure"
        )
        planning_error = reason_code if exc is None else f"{type(exc).__name__}: {exc}"
        exception_type = None if exc is None else type(exc).__name__
        exception_message = None if exc is None else str(exc)
        failure_reason_code = (
            "planner_sqlite_lock"
            if exception_type == "OperationalError" and "database is locked" in str(exception_message or "").lower()
            else "execution_planning_failed"
        )
        subphase = str(context.get("planner_subphase") or "execution_plan_batch_build")
        context.update(
            {
                "planning_error": planning_error,
                "failure_phase": "planner",
                "failure_subphase": subphase,
                "failure_reason_code": failure_reason_code,
                "exception_type": exception_type,
                "exception_message": exception_message,
            }
        )
        return ExecutionPlanningResult(
            context=context,
            execution_decision=execution_decision,
            execution_decision_summary=None,
            readiness_payload={},
            target_policy_metadata={},
            planning_error=planning_error,
            failure_phase="planner",
            failure_subphase=subphase,
            failure_reason_code=failure_reason_code,
            exception_type=exception_type,
            exception_message=exception_message,
        )

def _primary_submit_plan(
    summary: ExecutionDecisionSummary | None,
) -> ExecutionSubmitPlan | None:
    if summary is None:
        return None
    return (
        summary.typed_target_submit_plan()
        or summary.typed_residual_submit_plan()
        or summary.typed_buy_submit_plan()
    )


def _with_h74_submit_plan_evidence(
    submit_plan: ExecutionSubmitPlan | None,
    *,
    context: Mapping[str, object],
    readiness_payload: Mapping[str, object],
) -> ExecutionSubmitPlan | None:
    if submit_plan is None:
        return None
    extra = dict(submit_plan.extra_payload)
    for h74_key in (
        "position_mode",
        "hold_policy",
        "authority_hash",
        "authority_parameter_hash",
        "source_artifact_hash",
        "h74_source_authority_hash",
        "strategy_instance_id",
        "residual_inventory_mode",
        "partial_fill_policy",
        "cycle_id",
        "h74_cycle_id",
        "remaining_cycle_qty",
        "h74_remaining_cycle_qty",
        "locked_exit_qty",
        "h74_cycle_inventory_error",
        "h74_open_cycle_count",
        "h74_cycle_inventory",
        "h74_startup_gate_status",
        "h74_startup_gate_reason_code",
        "startup_gate_hash",
        "startup_gate",
        "contract_hash",
        "experiment_execution_contract",
        "h74_source_authority",
    ):
        if h74_key in H74_SUBMIT_SEMANTIC_FIELDS:
            continue
        if h74_key in extra:
            continue
        if h74_key in readiness_payload:
            extra[h74_key] = readiness_payload[h74_key]
        elif h74_key in context:
            extra[h74_key] = context[h74_key]
    if extra == dict(submit_plan.extra_payload):
        return submit_plan
    return replace(submit_plan, extra_payload=extra)


def _summary_with_primary_submit_plan(
    summary: ExecutionDecisionSummary | None,
    submit_plan: ExecutionSubmitPlan | None,
) -> ExecutionDecisionSummary | None:
    if summary is None or submit_plan is None:
        return summary
    if not isinstance(summary, ExecutionDecisionSummary):
        return summary
    if summary.typed_target_submit_plan() is not None:
        return replace(summary, target_submit_plan=submit_plan)
    if summary.typed_residual_submit_plan() is not None:
        return replace(summary, residual_submit_plan=submit_plan)
    if summary.typed_buy_submit_plan() is not None:
        return replace(summary, buy_submit_plan=submit_plan)
    return summary


def _base_currency_from_pair(pair: str) -> str:
    text = str(pair or "").strip()
    if "-" in text:
        return text.split("-", 1)[1].upper()
    return text[-3:].upper() if len(text) >= 3 else text.upper()


def _build_execution_plan_batch_for_runtime_pair(
    conn,
    *,
    context: Mapping[str, object],
    submit_plan: ExecutionSubmitPlan | None,
    updated_ts: int,
    read_only: bool = False,
) -> ExecutionPlanBatch:
    runtime_pair = str(context.get("runtime_pair") or getattr(settings, "PAIR", "") or "").strip()
    if not runtime_pair and submit_plan is not None:
        runtime_pair = str(submit_plan.pair or "").strip()
    if not runtime_pair:
        runtime_pair = "unknown"
    portfolio_target_payload = (
        dict(context.get("portfolio_target") or {})
        if isinstance(context.get("portfolio_target"), Mapping)
        else {}
    )
    target_scope_hashes = tuple(
        str(item).strip()
        for item in (portfolio_target_payload.get("scope_key_hashes") or ())
        if str(item).strip()
    )
    portfolio_target_hash = str(
        context.get("portfolio_target_hash")
        or (submit_plan.portfolio_target_hash if submit_plan is not None else "")
        or sha256_prefixed({"portfolio_target": "missing", "runtime_pair": runtime_pair})
    )
    submit_hash = (
        submit_plan.content_hash()
        if submit_plan is not None
        else sha256_prefixed(
            {
                "submit_expected": False,
                "runtime_pair": runtime_pair,
                "block_reason": str(context.get("execution_block_reason") or context.get("final_reason") or ""),
            }
        )
    )
    idempotency_key = str(
        (submit_plan.idempotency_key if submit_plan is not None else "")
        or context.get("submit_plan_idempotency_key")
        or sha256_prefixed({"runtime_pair": runtime_pair, "submit_plan_hash": submit_hash})
    )
    side = str(submit_plan.side if submit_plan is not None else context.get("authoritative_execution_signal") or "HOLD").upper()
    lock_evidence: dict[str, object]
    lock_intent: dict[str, object] | None = None
    db_locking_available = False if read_only else _batch_lock_tables_available(conn)
    if submit_plan is not None and bool(submit_plan.submit_expected) and side in {"BUY", "SELL"}:
        lock_type = "quote_budget" if side == "BUY" else "base_order"
        amount = float(submit_plan.notional_krw or 0.0) if side == "BUY" else float(submit_plan.qty or 0.0)
        reason = (
            "execution_plan_batch_buy_budget_lock"
            if side == "BUY"
            else "execution_plan_batch_sell_order_lock"
        )
        currency = "KRW" if side == "BUY" else _base_currency_from_pair(runtime_pair)
        evidence = {
            "execution_submit_plan_hash": submit_hash,
            "portfolio_target_hash": portfolio_target_hash,
            "runtime_pair": runtime_pair,
        }
        lock_table = "budget_locks" if side == "BUY" else "order_locks"
        lock_hash_payload = {
            "schema_version": 1,
            "lock_table": lock_table,
            **(
                {"currency": currency, "pair": runtime_pair}
                if side == "BUY"
                else {"pair": runtime_pair, "currency": currency}
            ),
            "amount": amount,
            "reason": reason,
            "idempotency_key": idempotency_key,
            "evidence": evidence,
        }
        evidence_hash = sha256_prefixed(lock_hash_payload)
        lock_intent = {
            "lock_kind": "budget" if side == "BUY" else "order",
            "currency": currency,
            "pair": runtime_pair,
            "amount": amount,
            "reason": reason,
            "created_ts": int(updated_ts),
            "idempotency_key": idempotency_key,
            "evidence": evidence,
            "lock_hash": evidence_hash,
            "evidence_hash": evidence_hash,
        }
        lock_evidence = {
            "lock_hash": evidence_hash,
            "lock_type": lock_type,
            "lock_status": "intent_pending_persistence",
            "evidence_hash": evidence_hash,
            "lock_intent": dict(lock_intent),
        }
    else:
        evidence_hash = sha256_prefixed(
            {
                "schema_version": 1,
                "lock_required": bool(submit_plan is not None and bool(submit_plan.submit_expected) and side in {"BUY", "SELL"}),
                "lock_persistence": (
                    "read_only_diagnostic"
                    if read_only
                    else "db_unavailable_diagnostic"
                    if not db_locking_available
                    else "not_required"
                ),
                "runtime_pair": runtime_pair,
                "execution_submit_plan_hash": submit_hash,
                "submit_expected": False if submit_plan is None else bool(submit_plan.submit_expected),
                "side": side,
            }
        )
        lock_evidence = {
            "lock_hash": evidence_hash,
            "lock_type": (
                "quote_budget"
                if side == "BUY" and submit_plan is not None and bool(submit_plan.submit_expected)
                else "base_order"
                if side == "SELL" and submit_plan is not None and bool(submit_plan.submit_expected)
                else "none"
            ),
            "lock_status": "diagnostic_unpersisted" if not db_locking_available else "not_required",
            "evidence_hash": evidence_hash,
        }
    if isinstance(context, dict):
        intents = list(context.get("lock_intents") or [])
        if lock_intent is not None:
            intents.append(dict(lock_intent))
        context["lock_intents"] = intents
        context["required_lock_evidence"] = lock_evidence
    execution_order_rules = resolve_execution_order_rules(dict(context), market=runtime_pair)
    order_rule_snapshot = order_rules_snapshot_payload(
        execution_order_rules.as_order_rules(),
        pair=runtime_pair,
    )
    order_rule_snapshot_hash = sha256_prefixed(order_rule_snapshot)
    order_rule_signature = sha256_prefixed(
        {
            "pair": runtime_pair,
            "order_rule_snapshot_hash": order_rule_snapshot_hash,
            "order_rule_authority_source": order_rule_snapshot.get("order_rule_authority_source"),
            "order_rule_authority_source_mode": order_rule_snapshot.get("order_rule_authority_source_mode"),
        }
    )
    raw_pre_submit_hash = str(
        context.get("pre_submit_risk_decision_hash")
        or (
            submit_plan.extra_payload.get("pre_submit_risk_decision_hash")
            if submit_plan is not None and isinstance(submit_plan.extra_payload, dict)
            else ""
        )
        or ""
    ).strip()
    submit_expected = bool(submit_plan is not None and bool(submit_plan.submit_expected))
    pre_submit_required = bool(
        submit_expected
        and (
            (submit_plan is not None and bool(submit_plan.extra_payload.get("pre_submit_risk_required")))
            or bool(context.get("pre_submit_risk_required"))
            or (
                run_loop_uses_target_delta(settings)
                and str(getattr(settings, "MODE", "") or "").strip().lower() == "live"
                and not bool(getattr(settings, "LIVE_DRY_RUN", True))
                and bool(getattr(settings, "LIVE_REAL_ORDER_ARMED", False))
                and submit_plan is not None
                and str(submit_plan.source or "") == "target_delta"
            )
        )
    )
    pre_submit_not_required_reason = ""
    pre_submit_finalization_required = False
    pre_submit_status = str(context.get("pre_submit_risk_status") or "").strip()
    if pre_submit_required and not raw_pre_submit_hash:
        pre_submit_finalization_required = submit_expected
        pre_submit_status = pre_submit_status or "pending_finalization"
    elif not pre_submit_required:
        if submit_plan is None:
            pre_submit_not_required_reason = "no_submit_plan"
        elif not bool(submit_plan.submit_expected):
            pre_submit_not_required_reason = str(submit_plan.block_reason or "submit_not_expected")
        else:
            pre_submit_not_required_reason = "not_live_real_submit_path"
        pre_submit_status = pre_submit_status or "not_required"
    pre_submit_hash = raw_pre_submit_hash
    pair_plan = PairExecutionPlan(
        pair=runtime_pair,
        scope_key_hash=str(context.get("scope_key_hash") or (target_scope_hashes[0] if target_scope_hashes else "")),
        scope_key_hashes=target_scope_hashes,
        portfolio_target_hash=portfolio_target_hash,
        execution_submit_plan_hash=submit_hash,
        execution_plan_hash=str(context.get("execution_plan_bundle_hash") or ""),
        order_rule_snapshot_hash=order_rule_snapshot_hash,
        order_rule_signature=order_rule_signature,
        order_rule_snapshot=order_rule_snapshot,
        idempotency_key=idempotency_key,
        submit_authority_policy_hash=str(
            (submit_plan.submit_authority_policy_hash if submit_plan is not None else "")
            or context.get("submit_authority_policy_hash")
            or sha256_prefixed({"submit_authority_policy": "compatibility"})
        ),
        pre_submit_risk_decision_hash=pre_submit_hash,
        pre_submit_risk_required=pre_submit_required,
        pre_submit_risk_proof_status=pre_submit_status,
        pre_submit_risk_not_required_reason=pre_submit_not_required_reason,
        pre_submit_risk_finalization_required=pre_submit_finalization_required,
        submit_expected=submit_expected,
        lock_evidence_hash=str(lock_evidence["evidence_hash"]),
        lock_type=str(lock_evidence["lock_type"]),
        lock_status=str(lock_evidence["lock_status"]),
        replay_evidence={
            "execution_submit_plan_hash": submit_hash,
            "portfolio_target_hash": portfolio_target_hash,
            "scope_key_hashes": list(target_scope_hashes),
            "order_rule_snapshot_hash": order_rule_snapshot_hash,
            "order_rule_signature": order_rule_signature,
            "pre_submit_risk_decision_hash": pre_submit_hash,
            "pre_submit_risk_required": pre_submit_required,
            "pre_submit_risk_not_required_reason": pre_submit_not_required_reason,
            "pre_submit_risk_finalization_required": pre_submit_finalization_required,
            "submit_plan_source": "" if submit_plan is None else submit_plan.source,
            "submit_plan_authority": "" if submit_plan is None else submit_plan.authority,
        },
    )
    batch_risk_evidence = {
        "schema_version": 1,
        "risk_scope": "batch_size_one_single_pair",
        "runtime_pair": runtime_pair,
        "pair_plan_hashes": [pair_plan.content_hash()],
        "lock_evidence_hashes": [str(lock_evidence["evidence_hash"])],
        "pre_submit_risk_decision_hash": pre_submit_hash,
        "pre_submit_risk_required": pre_submit_required,
        "pre_submit_risk_not_required_reason": pre_submit_not_required_reason,
        "pre_submit_risk_finalization_required": pre_submit_finalization_required,
        "status": "ALLOW" if submit_expected else "NOT_REQUIRED",
    }
    return ExecutionPlanBatch(
        runtime_strategy_set_manifest_hash=str(
            context.get("runtime_strategy_set_manifest_hash")
            or sha256_prefixed({"runtime_strategy_set_manifest": "missing_compatibility"})
        ),
        allocation_decision_hash=str(
            context.get("allocation_decision_hash")
            or sha256_prefixed({"allocation_decision": "missing_compatibility", "runtime_pair": runtime_pair})
        ),
        pair_plans=(pair_plan,),
        batch_risk_decision_evidence=batch_risk_evidence,
        budget_lock_hash=sha256_prefixed(
            {
                "schema_version": 1,
                "batch_lock_evidence": [lock_evidence],
                "pair_plan_hashes": [pair_plan.content_hash()],
            }
        ),
    )


def _batch_lock_tables_available(conn) -> bool:
    if not callable(getattr(conn, "execute", None)):
        return False
    try:
        rows = conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name IN ('budget_locks', 'order_locks')
            """
        ).fetchall()
    except Exception:
        return False
    names = {str(row["name"] if hasattr(row, "keys") else row[0]) for row in rows}
    return {"budget_locks", "order_locks"}.issubset(names)


def _plan_status(planning: ExecutionPlanningResult) -> ExecutionPlanStatus:
    if planning.planning_error is not None:
        return ExecutionPlanStatus(
            status="ERROR",
            reason_code="execution_planning_error",
            reason=planning.planning_error,
        )
    if planning.execution_decision_summary is None:
        return ExecutionPlanStatus(
            status="ERROR",
            reason_code="execution_summary_missing",
            reason="execution decision summary was not produced",
        )
    if not bool(planning.execution_decision_summary.submit_expected):
        return ExecutionPlanStatus(
            status="BLOCKED",
            reason_code=str(planning.execution_decision_summary.block_reason or "submit_not_expected"),
            reason=str(planning.execution_decision_summary.block_reason or "submit_not_expected"),
        )
    return ExecutionPlanStatus(status="PLANNED", reason_code="none", reason="none")
