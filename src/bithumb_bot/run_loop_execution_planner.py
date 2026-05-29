from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Callable, Mapping

from .config import settings
from .db_core import load_target_position_state, upsert_target_position_state
from .decision_envelope import DecisionEnvelope, _thaw_mapping
from .decision_equivalence import sha256_prefixed
from .execution_order_rules import resolve_execution_order_rules
from .execution_service import (
    ExecutionDecisionSummary,
    ExecutionReadinessPlanningInput,
    ExecutionSubmitPlan,
    ExecutionTargetPlanningInput,
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
from .runtime_readiness import compute_runtime_readiness_snapshot
from .runtime_strategy_set import (
    RuntimeStrategyDecisionResultBundle,
    RuntimeStrategySet,
    derive_strategy_instance_id,
    runtime_strategy_set_manifest_hash,
)
from .strategy_policy_contract import StrategyDecisionV2
from .strategy_performance import evaluate_strategy_performance_gate
from .target_position import (
    TARGET_POLICY_ADOPT_EXISTING_BROKER_POSITION,
    TARGET_POLICY_INITIALIZE_FLAT_TARGET,
    TARGET_POLICY_INITIALIZE_TRUE_DUST_FLAT,
    TARGET_POLICY_USE_EXISTING_TARGET,
    resolve_startup_target_position_policy,
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
    "cash_available",
)


@dataclass(frozen=True)
class ExecutionPlanningResult:
    context: dict[str, object]
    execution_decision: dict[str, object]
    execution_decision_summary: ExecutionDecisionSummary | None
    readiness_payload: dict[str, object]
    target_policy_metadata: dict[str, object]
    planning_error: str | None = None


@dataclass(frozen=True)
class ExecutionPlanBundle:
    summary: ExecutionDecisionSummary | None
    submit_plan: ExecutionSubmitPlan | None
    persistence_context: dict[str, object]
    readiness_payload: dict[str, object]
    target_policy_metadata: dict[str, object]
    planning_error: str | None = None
    status: "ExecutionPlanStatus | None" = None

    def as_dict(self) -> dict[str, object]:
        submit_plan = None if self.submit_plan is None else self.submit_plan.as_dict()
        summary = None if self.summary is None else self.summary.as_dict()
        status = None if self.status is None else self.status.as_dict()
        return {
            "schema_version": 1,
            "authority_label": "ExecutionPlanBundle",
            "summary_authority": "ExecutionDecisionSummary" if self.summary is not None else "missing",
            "submit_plan_authority": "ExecutionSubmitPlan" if self.submit_plan is not None else "none",
            "summary": summary,
            "primary_submit_plan": submit_plan,
            "status": status,
            "planning_error": self.planning_error,
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


def _allocator_target_exposure_krw() -> float:
    explicit = getattr(settings, "TARGET_EXPOSURE_KRW", None)
    if explicit is not None:
        try:
            return max(0.0, float(explicit))
        except (TypeError, ValueError):
            pass
    return max(0.0, float(getattr(settings, "MAX_ORDER_KRW", 0.0) or 0.0))


def _allocation_context_fields(decision) -> dict[str, object]:
    target = decision.target_for_pair(str(settings.PAIR))
    target_payload = None if target is None else target.as_dict()
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
        "portfolio_allocation_decision": decision.as_dict(),
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


def run_loop_uses_target_delta() -> bool:
    return (
        str(getattr(settings, "EXECUTION_ENGINE", "lot_native") or "lot_native").strip().lower()
        == "target_delta"
    )


def load_previous_target_exposure_for_run_loop(conn) -> float | None:
    if not run_loop_uses_target_delta():
        return None
    previous_target_state = load_target_position_state(conn, pair=settings.PAIR)
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
) -> dict[str, object]:
    if not run_loop_uses_target_delta():
        return {
            "previous_target_exposure_krw": None,
            "target_policy_metadata": {},
            "target_state": None,
        }
    previous_target_state = load_target_position_state(conn, pair=settings.PAIR)
    execution_order_rules = resolve_execution_order_rules(readiness_payload, market=str(settings.PAIR))
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
        upsert_target_position_state(
            conn,
            pair=settings.PAIR,
            target_exposure_krw=float(policy.target_exposure_krw or 0.0),
            target_qty=float(policy.target_qty or 0.0),
            last_signal=str(raw_signal or "HOLD").upper(),
            last_decision_id=None,
            last_reference_price=float(reference_price or 0.0),
            updated_ts=int(updated_ts),
            target_origin=policy.target_origin,
            adoption_reason=policy.adoption_reason,
            adopted_broker_qty=policy.adopted_broker_qty,
            adopted_broker_exposure_krw=policy.adopted_broker_exposure_krw,
            created_from_signal=policy.created_from_signal,
        )
        previous_target_state = load_target_position_state(conn, pair=settings.PAIR)
    previous_exposure = (
        None if previous_target_state is None else float(previous_target_state.target_exposure_krw)
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


def _live_real_target_delta_performance_gate_applies() -> bool:
    return bool(
        run_loop_uses_target_delta()
        and str(settings.MODE).strip().lower() == "live"
        and bool(settings.LIVE_REAL_ORDER_ARMED)
        and not bool(settings.LIVE_DRY_RUN)
    )


def _live_real_order_submit_plan_required() -> bool:
    return bool(
        str(settings.MODE).strip().lower() == "live"
        and bool(settings.LIVE_REAL_ORDER_ARMED)
        and not bool(settings.LIVE_DRY_RUN)
    )


@dataclass(frozen=True)
class ExecutionPlanner:
    readiness_snapshot_builder: Callable[..., object] = compute_runtime_readiness_snapshot
    performance_gate_evaluator: Callable[..., object] = evaluate_strategy_performance_gate
    summary_builder: Callable[..., ExecutionDecisionSummary] = build_typed_execution_decision_summary
    target_state_resolver: Callable[..., dict[str, object]] = resolve_target_position_state_for_run_loop
    persistence_context_builder: Callable[..., dict[str, object]] = prepare_strategy_decision_persistence_context
    strict_promotion_mode: bool = True

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
        submit_plan = _primary_submit_plan(planning.execution_decision_summary)
        context = dict(planning.context)
        context.update(
            {
                "decision_authority_source": "DecisionEnvelope.strategy_decision",
                "decision_envelope_present": True,
                "execution_plan_bundle_present": True,
                "submit_plan_source": None if submit_plan is None else submit_plan.source,
                "submit_plan_authority": None if submit_plan is None else submit_plan.authority,
                "persistence_context_authoritative": 0,
                "non_authoritative_observability_payload": True,
            }
        )
        bundle = ExecutionPlanBundle(
            summary=planning.execution_decision_summary,
            submit_plan=submit_plan,
            persistence_context=context,
            readiness_payload=planning.readiness_payload,
            target_policy_metadata=planning.target_policy_metadata,
            planning_error=planning.planning_error,
            status=_plan_status(planning),
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
        representative = result_bundle.results[0]
        envelope = DecisionEnvelope.from_runtime_result(representative)
        planning_input = ExecutionPlanningInput.from_envelope(envelope)
        planning = self._plan_typed_input(
            conn,
            planning_input=planning_input,
            updated_ts=int(updated_ts),
            runtime_result_bundle=result_bundle,
        )
        submit_plan = _primary_submit_plan(planning.execution_decision_summary)
        context = dict(planning.context)
        context.update(
            {
                "decision_authority_source": "PortfolioAllocator.portfolio_target",
                "representative_strategy_decision_authority": "non_authoritative_observability_only",
                "decision_envelope_present": True,
                "execution_plan_bundle_present": True,
                "submit_plan_source": None if submit_plan is None else submit_plan.source,
                "submit_plan_authority": None if submit_plan is None else submit_plan.authority,
                "persistence_context_authoritative": 0,
                "non_authoritative_observability_payload": True,
            }
        )
        bundle = ExecutionPlanBundle(
            summary=planning.execution_decision_summary,
            submit_plan=submit_plan,
            persistence_context=context,
            readiness_payload=planning.readiness_payload,
            target_policy_metadata=planning.target_policy_metadata,
            planning_error=planning.planning_error,
            status=_plan_status(planning),
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
        execution_intent = getattr(decision, "execution_intent", None)
        if execution_intent is not None and hasattr(execution_intent, "as_dict"):
            context["execution_intent"] = execution_intent.as_dict()
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
            context.update(_runtime_strategy_set_context_fields(strategy_set))
            context.update(_runtime_result_bundle_context_fields(runtime_result_bundle))
            readiness_payload = self.readiness_snapshot_builder(conn).as_dict()
            strategy_performance_gate = None
            if _live_real_target_delta_performance_gate_applies():
                strategy_performance_gate = self.performance_gate_evaluator(
                    conn,
                    strategy_name=str(settings.STRATEGY_NAME),
                    pair=str(settings.PAIR),
                )
            reference_price = context.get("market_price", context.get("last_close", context.get("close")))
            target_resolution = self.target_state_resolver(
                conn,
                readiness_payload=readiness_payload,
                reference_price=reference_price,
                raw_signal=planning_input.final_signal,
                updated_ts=int(updated_ts),
            )
            previous_target_exposure_krw = target_resolution.get("previous_target_exposure_krw")
            target_policy_metadata = dict(target_resolution.get("target_policy_metadata", {}))
            allocation_config = PortfolioAllocatorConfig(
                target_exposure_krw=_allocator_target_exposure_krw(),
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
            if runtime_result_bundle is None:
                strategy_preference = strategy_decision_to_preference(
                    planning_input.strategy_decision,
                    pair=str(settings.PAIR),
                    strategy_instance_id=str(
                        context.get("strategy_instance_id")
                        or planning_input.strategy_decision.strategy_name
                    ),
                    desired_exposure_krw=_allocator_target_exposure_krw(),
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
                                    "parameter_source",
                                )
                                if key in result_context
                            }
                        )
                    runtime_result_contexts.append(result_metadata)
                    preference_list.append(
                        strategy_decision_to_preference(
                            result.decision,
                            pair=str(spec.pair),
                            strategy_instance_id=strategy_instance_id,
                            desired_exposure_krw=spec.desired_exposure_krw,
                            desired_weight=spec.weight,
                            risk_budget_krw=spec.risk_budget_krw,
                            metadata=result_metadata,
                        )
                    )
                context["runtime_strategy_result_contexts"] = runtime_result_contexts
                preferences = tuple(preference_list)
            context["strategy_preference_count"] = len(preferences)
            context["strategy_preferences"] = [item.as_dict() for item in preferences]
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
            )
            allocation_decision = PortfolioAllocator(allocation_config).allocate(allocation_input)
            portfolio_target = allocation_decision.target_for_pair(str(settings.PAIR))
            allocation_context = _allocation_context_fields(allocation_decision)
            context.update(allocation_context)
            authoritative_signal = str(
                context.get("allocation_selected_signal") or planning_input.final_signal or "HOLD"
            ).upper()
            if runtime_result_bundle is not None and authoritative_signal in {"BUY", "SELL", "HOLD"}:
                planning_input = replace(
                    planning_input,
                    strategy_decision=replace(
                        planning_input.strategy_decision,
                        raw_signal=authoritative_signal,
                        final_signal=authoritative_signal,
                        final_reason=str(context.get("allocator_reason") or planning_input.final_reason),
                    ),
                )
                context["signal"] = authoritative_signal
                context["raw_signal"] = authoritative_signal
                context["final_signal"] = authoritative_signal
                context["final_reason"] = str(context.get("allocator_reason") or planning_input.final_reason)
                context["authoritative_execution_signal"] = authoritative_signal
            else:
                context["authoritative_execution_signal"] = planning_input.final_signal
            readiness_payload = {**readiness_payload, **target_policy_metadata}
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

    def plan_strategy_decision(
        self,
        conn,
        *,
        decision_context: dict[str, object],
        signal: str,
        reason: str,
        updated_ts: int,
        allow_legacy_context_planning: bool = False,
    ) -> ExecutionPlanningResult:
        return self._fail_closed_context(
            decision_context=decision_context,
            reason_code=(
                "legacy_context_planning_diagnostic_only"
                if allow_legacy_context_planning
                else "legacy_context_planning_disabled"
            ),
        )

    def plan_diagnostic_legacy_context(
        self,
        conn,
        *,
        decision_context: dict[str, object],
        signal: str,
        reason: str,
        updated_ts: int,
        allow_legacy_context_planning: bool = False,
    ) -> ExecutionPlanningResult:
        if not allow_legacy_context_planning:
            return self._fail_closed_context(
                decision_context=decision_context,
                reason_code="legacy_context_planning_disabled",
            )
        if _live_real_order_submit_plan_required():
            return self._fail_closed_context(
                decision_context=decision_context,
                reason_code="legacy_context_planning_live_real_order_disabled",
            )
        return self._plan_diagnostic_context(
            conn,
            decision_context=decision_context,
            signal=signal,
            reason=reason,
            raw_signal=None,
            updated_ts=updated_ts,
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
        return ExecutionPlanningResult(
            context=context,
            execution_decision=execution_decision,
            execution_decision_summary=None,
            readiness_payload={},
            target_policy_metadata={},
            planning_error=planning_error,
        )

    def _plan_diagnostic_context(
        self,
        conn,
        *,
        decision_context: dict[str, object],
        signal: str,
        reason: str,
        raw_signal: str | None,
        updated_ts: int,
        typed_planning_input: ExecutionPlanningInput | None = None,
    ) -> ExecutionPlanningResult:
        context = dict(decision_context)
        try:
            readiness_payload = self.readiness_snapshot_builder(conn).as_dict()
            strategy_performance_gate = None
            if _live_real_target_delta_performance_gate_applies():
                strategy_performance_gate = self.performance_gate_evaluator(
                    conn,
                    strategy_name=str(settings.STRATEGY_NAME),
                    pair=str(settings.PAIR),
                )
            raw_signal_for_target = str(
                raw_signal or context.get("raw_signal") or context.get("base_signal") or signal
            )
            reference_price = context.get("market_price", context.get("last_close", context.get("close")))
            target_resolution = self.target_state_resolver(
                conn,
                readiness_payload=readiness_payload,
                reference_price=reference_price,
                raw_signal=raw_signal_for_target,
                updated_ts=int(updated_ts),
            )
            previous_target_exposure_krw = target_resolution.get("previous_target_exposure_krw")
            target_policy_metadata = dict(target_resolution.get("target_policy_metadata", {}))
            readiness_payload = {**readiness_payload, **target_policy_metadata}
            summary_context = dict(context)
            if typed_planning_input is not None:
                from .execution_service import build_execution_decision_summary

                summary_context = self._planning_context_from_envelope_input(typed_planning_input)
                typed_builder = (
                    build_typed_execution_decision_summary
                    if self.summary_builder is build_execution_decision_summary
                    else self.summary_builder
                )
                execution_decision_summary = typed_builder(
                    typed_input=TypedExecutionPlanningInput(
                        strategy_decision=typed_planning_input.strategy_decision,
                        candle_ts=typed_planning_input.candle_ts,
                        market_price=typed_planning_input.market_price,
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
                        ),
                        observability_context=summary_context,
                    ),
                    strategy_performance_gate=strategy_performance_gate,
                )
            else:
                from .execution_service import build_execution_decision_summary

                legacy_summary_kwargs = {
                    "decision_context": summary_context,
                    "readiness_payload": readiness_payload,
                    "raw_signal": raw_signal_for_target,
                    "final_signal": signal,
                    "final_reason": reason,
                    "previous_target_exposure_krw": (
                        None
                        if previous_target_exposure_krw is None
                        else float(previous_target_exposure_krw)
                    ),
                    "strategy_performance_gate": strategy_performance_gate,
                }
                legacy_builder = (
                    build_execution_decision_summary
                    if self.summary_builder is build_typed_execution_decision_summary
                    else self.summary_builder
                )
                execution_decision_summary = legacy_builder(**legacy_summary_kwargs)
                summary_context["legacy_context_planning_used"] = True
                summary_context["compatibility_fallback"] = True
                summary_context["promotion_grade"] = False
                summary_context["recommended_next_action"] = (
                    "regenerate_decision_with_typed_execution_authority"
                )
            context = self.persistence_context_builder(
                decision_context=summary_context,
                execution_decision_summary=execution_decision_summary,
                readiness_payload=readiness_payload,
                target_policy_metadata=target_policy_metadata,
            )
            execution_decision = dict(context["execution_decision"])  # type: ignore[arg-type]
            return ExecutionPlanningResult(
                context=context,
                execution_decision=execution_decision,
                execution_decision_summary=execution_decision_summary,
                readiness_payload=readiness_payload,
                target_policy_metadata=target_policy_metadata,
            )
        except Exception as exc:
            return self._fail_closed_context(
                decision_context=context,
                reason_code="execution_decision_unavailable",
                exc=exc,
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
