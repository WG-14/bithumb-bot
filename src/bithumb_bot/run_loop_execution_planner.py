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


def _runtime_pair_for_planning(strategy_set: RuntimeStrategySet | None) -> str:
    del strategy_set
    return str(settings.PAIR)


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
        "runtime_scope": "multi-strategy / single-pair runtime",
        "runtime_scope_mode": "single_pair",
        "multi_pair_portfolio_supported": False,
        "multi_pair_portfolio_fail_closed_reason": "multi_pair_runtime_unsupported",
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
        raw = evaluator(
            conn,
            strategy_name=str(contribution.strategy_name),
            pair=str(contribution.pair),
        )
        payload = _gate_payload(raw)
        item = {
            "strategy_instance_id": contribution.strategy_instance_id,
            "strategy_name": contribution.strategy_name,
            "pair": contribution.pair,
            "selected_signal": contribution.signal_direction,
            "gate": payload,
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

    @property
    def result_cls(self) -> type[ExecutionPlanningResult]:
        return ExecutionPlanningResult

    @property
    def typed_summary_builder(self) -> Callable[..., ExecutionDecisionSummary]:
        return build_typed_execution_decision_summary

    @staticmethod
    def live_real_target_delta_performance_gate_applies() -> bool:
        return _live_real_target_delta_performance_gate_applies()

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
        submit_plan = _primary_submit_plan(planning.execution_decision_summary)
        context = dict(planning.context)
        context.update(
            {
                "decision_authority_source": "PortfolioAllocator.portfolio_target",
                "representative_strategy_decision_authority": "non_authoritative_observability_only",
                "decision_envelope_present": not result_bundle.strategy_set.multi_strategy_enabled,
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
            reference_price = context.get("market_price", context.get("last_close", context.get("close")))
            if runtime_result_bundle is None:
                target_resolution = self.target_state_resolver(
                    conn,
                    readiness_payload=readiness_payload,
                    reference_price=reference_price,
                    raw_signal=planning_input.final_signal,
                    updated_ts=int(updated_ts),
                )
                previous_target_exposure_krw = target_resolution.get("previous_target_exposure_krw")
                target_policy_metadata = dict(target_resolution.get("target_policy_metadata", {}))
            else:
                try:
                    previous_target_exposure_krw = load_previous_target_exposure_for_run_loop(conn)
                except AttributeError:
                    previous_target_exposure_krw = None
                target_policy_metadata = {}
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
                            risk_budget_krw=spec.max_target_exposure_krw,
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
            allocation_context = _allocation_context_fields(allocation_decision)
            context.update(allocation_context)
            runtime_pair = _runtime_pair_for_planning(strategy_set)
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
            if _live_real_target_delta_performance_gate_applies() and allocation_authoritative:
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
                target_resolution = self.target_state_resolver(
                    conn,
                    readiness_payload=readiness_payload,
                    reference_price=reference_price,
                    raw_signal=authoritative_signal,
                    updated_ts=int(updated_ts),
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
