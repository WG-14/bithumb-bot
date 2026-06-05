from __future__ import annotations

import logging
from dataclasses import dataclass, field, replace
from typing import Callable, Mapping, Protocol

from . import runtime_state
from .config import settings
from .db_core import ensure_db
from .decision_contract import apply_decision_contract
from .decision_context import resolve_canonical_position_exposure_snapshot
from .decision_equivalence import sha256_prefixed
from .execution_order_rules import resolve_execution_order_rules
from .observability import format_log_kv
from .oms import build_order_intent_key
from .order_sizing import build_target_delta_execution_sizing
from .portfolio_target import PortfolioTarget
from .risk_decision import build_risk_decision_artifact
from .pre_trade_economics import build_pre_trade_economics_snapshot
from .strategy_policy_contract import StrategyDecisionV2
from .submit_authority_policy import (
    evaluate_submit_authority_policy,
    submit_authority_policy_from_settings,
)
from .target_position import TargetPositionSettings, build_target_position_decision

if False:  # pragma: no cover
    from .broker.base import Broker

RUN_LOG = logging.getLogger("bithumb_bot.run")
EXECUTION_SUBMIT_PLAN_SCHEMA_VERSION = 1
EXECUTION_SUBMIT_PLAN_AUTHORITY_LABEL = "ExecutionSubmitPlan.final_payload.v1"


EXECUTION_PLANNING_READINESS_KEYS = frozenset(
    {
        "residual_inventory_mode",
        "residual_inventory_state",
        "residual_inventory_policy_allows_run",
        "residual_inventory_policy_allows_buy",
        "residual_inventory_policy_allows_sell",
        "residual_inventory",
        "residual_sell_candidate",
        "projection_converged",
        "projection_convergence",
        "open_order_count",
        "unresolved_open_order_count",
        "recovery_required_count",
        "submit_unknown_count",
        "broker_position_evidence",
        "total_effective_exposure_qty",
        "total_effective_exposure_notional_krw",
        "residual_inventory_notional_krw",
        "min_qty",
        "qty_step",
        "min_notional_krw",
        "bid_min_total_krw",
        "bid_types",
        "residual_proof_min_qty",
        "residual_proof_min_notional_krw",
        "residual_proof_locked_qty",
        "active_fee_accounting_blocker",
        "accounting_projection_ok",
        "idempotency_scope",
        "cash_available",
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
    }
)


@dataclass(frozen=True)
class ExecutionObservabilityPayload:
    payload: Mapping[str, object] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {str(key): value for key, value in dict(self.payload).items()}


@dataclass(frozen=True)
class TypedExecutionRequest:
    signal: str
    ts: int
    market_price: float
    strategy_name: str | None = None
    decision_id: int | None = None
    decision_reason: str | None = None
    exit_rule_name: str | None = None
    execution_decision_summary: "ExecutionDecisionSummary | None" = None
    execution_plan_bundle: object | None = None
    observability_payload: ExecutionObservabilityPayload | None = None
    research_execution_context: object | None = None

    def __post_init__(self) -> None:
        if self.execution_decision_summary is not None and not isinstance(
            self.execution_decision_summary, ExecutionDecisionSummary
        ):
            raise TypeError("execution_decision_summary_must_be_typed")
        if _live_real_order_submit_plan_required():
            typed_summary = self.execution_decision_summary or getattr(
                self.execution_plan_bundle, "summary", None
            )
            if typed_summary is None:
                raise TypeError("live_real_order_missing_typed_execution_summary")
            if not isinstance(typed_summary, ExecutionDecisionSummary):
                raise TypeError("live_real_order_invalid_typed_execution_summary")
            bundle_plan = getattr(self.execution_plan_bundle, "submit_plan", None)
            if bundle_plan is not None and not isinstance(bundle_plan, ExecutionSubmitPlan):
                raise TypeError("live_real_order_invalid_execution_plan_bundle_submit_plan")


@dataclass(frozen=True)
class SignalExecutionRequest(TypedExecutionRequest):
    # Compatibility-only aliases. These dicts are non-authoritative
    # observability material and must not be used as live submit authority.
    decision_context: dict[str, object] | None = None
    observability_context: dict[str, object] | None = None

    def __post_init__(self) -> None:
        if _live_real_order_submit_plan_required():
            typed_summary = self.execution_decision_summary or getattr(
                self.execution_plan_bundle,
                "summary",
                None,
            )
            for field_name, payload in (
                ("decision_context", self.decision_context),
                ("observability_context", self.observability_context),
            ):
                if (
                    typed_summary is None
                    and isinstance(payload, dict)
                    and "execution_decision" in payload
                ):
                    raise TypeError(f"{field_name}_not_execution_authority")
        super().__post_init__()
        return None

    @classmethod
    def from_typed(
        cls,
        typed_request: TypedExecutionRequest,
        *,
        observability_payload: ExecutionObservabilityPayload | Mapping[str, object] | None = None,
    ) -> "SignalExecutionRequest":
        payload = (
            observability_payload
            if isinstance(observability_payload, ExecutionObservabilityPayload)
            else ExecutionObservabilityPayload(observability_payload or {})
        )
        return cls(
            signal=typed_request.signal,
            ts=typed_request.ts,
            market_price=typed_request.market_price,
            strategy_name=typed_request.strategy_name,
            decision_id=typed_request.decision_id,
            decision_reason=typed_request.decision_reason,
            exit_rule_name=typed_request.exit_rule_name,
            execution_decision_summary=typed_request.execution_decision_summary,
            execution_plan_bundle=typed_request.execution_plan_bundle,
            observability_payload=payload,
            research_execution_context=typed_request.research_execution_context,
        )


@dataclass(frozen=True)
class ExecutionReadinessPlanningInput:
    """Allowlisted readiness material used by the typed execution planner."""

    fields: Mapping[str, object] = field(default_factory=dict)

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, object] | None,
        *,
        target_policy_metadata: Mapping[str, object] | None = None,
    ) -> "ExecutionReadinessPlanningInput":
        merged: dict[str, object] = {}
        for source in (payload or {}, target_policy_metadata or {}):
            for key, value in source.items():
                if str(key) in EXECUTION_PLANNING_READINESS_KEYS:
                    merged[str(key)] = value
        return cls(fields=merged)

    def as_payload(self) -> dict[str, object]:
        return {key: self.fields[key] for key in sorted(self.fields)}


@dataclass(frozen=True)
class ExecutionTargetPlanningInput:
    previous_target_exposure_krw: float | None = None
    portfolio_target: PortfolioTarget | None = None
    portfolio_target_hash: str = ""
    allocation_decision_hash: str = ""
    allocator_config_hash: str = ""
    strategy_contribution_hash: str = ""


@dataclass(frozen=True)
class TypedExecutionPlanningInput:
    strategy_decision: StrategyDecisionV2
    candle_ts: int
    market_price: float
    readiness: ExecutionReadinessPlanningInput = field(
        default_factory=ExecutionReadinessPlanningInput
    )
    target: ExecutionTargetPlanningInput = field(default_factory=ExecutionTargetPlanningInput)
    observability_context: Mapping[str, object] = field(default_factory=dict)

    def as_authority_payload(self) -> dict[str, object]:
        decision = self.strategy_decision
        payload = self.readiness.as_payload()
        payload.update(
            {
                "ts": int(self.candle_ts),
                "candle_ts": int(self.candle_ts),
                "last_close": float(self.market_price),
                "market_price": float(self.market_price),
                "strategy": decision.strategy_name,
                "signal": decision.final_signal,
                "reason": decision.final_reason,
                "raw_signal": decision.raw_signal,
                "raw_reason": decision.raw_reason,
                "final_signal": decision.final_signal,
                "final_reason": decision.final_reason,
                "entry_block_reason": decision.entry_block_reason,
                "exit_rule": decision.exit_rule,
                "exit_reason": decision.exit_reason,
                "policy_contract_hash": decision.policy_contract_hash,
                "policy_input_hash": decision.policy_input_hash,
                "policy_decision_hash": decision.policy_decision_hash,
                "decision_authority_source": "TypedExecutionPlanningInput.strategy_decision",
                "persistence_context_authoritative": 0,
            }
        )
        position = decision.position_snapshot
        payload.update(
            {
                "entry_allowed": bool(position.entry_allowed),
                "exit_allowed": bool(position.exit_allowed),
                "exit_block_reason": position.exit_block_reason,
                "terminal_state": position.terminal_state,
                "qty_open": float(position.qty_open),
                "raw_qty_open": float(position.raw_qty_open),
                "raw_total_asset_qty": float(position.raw_total_asset_qty),
                "open_lot_count": int(position.open_lot_count),
                "dust_tracking_lot_count": int(position.dust_tracking_lot_count),
                "reserved_exit_lot_count": int(position.reserved_exit_lot_count),
                "sellable_executable_lot_count": int(position.sellable_executable_lot_count),
                "sellable_executable_qty": float(position.qty_open),
                "dust_classification": position.dust_classification,
                "dust_state": position.dust_state,
                "effective_flat": bool(position.effective_flat),
                "has_executable_exposure": bool(position.has_executable_exposure),
                "has_any_position_residue": bool(position.has_any_position_residue),
                "has_non_executable_residue": bool(position.has_non_executable_residue),
                "has_dust_only_remainder": bool(position.has_dust_only_remainder),
            }
        )
        normalized_exposure = {
            "semantic_basis": "lot-native",
            "entry_allowed": bool(position.entry_allowed),
            "exit_allowed": bool(position.exit_allowed),
            "entry_block_reason": position.entry_block_reason,
            "exit_block_reason": position.exit_block_reason,
            "terminal_state": position.terminal_state,
            "raw_qty_open": float(position.raw_qty_open),
            "raw_total_asset_qty": float(position.raw_total_asset_qty),
            "open_exposure_qty": float(position.qty_open),
            "dust_tracking_qty": 0.0,
            "reserved_exit_qty": 0.0,
            "open_lot_count": int(position.open_lot_count),
            "dust_tracking_lot_count": int(position.dust_tracking_lot_count),
            "reserved_exit_lot_count": int(position.reserved_exit_lot_count),
            "sellable_executable_lot_count": int(position.sellable_executable_lot_count),
            "sellable_executable_qty": float(position.qty_open),
            "dust_classification": position.dust_classification,
            "dust_state": position.dust_state,
            "effective_flat": bool(position.effective_flat),
            "normalized_exposure_active": bool(position.has_executable_exposure),
            "normalized_exposure_qty": float(position.qty_open),
            "has_executable_exposure": bool(position.has_executable_exposure),
            "has_any_position_residue": bool(position.has_any_position_residue),
            "has_non_executable_residue": bool(position.has_non_executable_residue),
            "has_dust_only_remainder": bool(position.has_dust_only_remainder),
        }
        payload["position_state"] = {
            "semantic_basis": "lot-native",
            "normalized_exposure": normalized_exposure,
        }
        if (
            "total_effective_exposure_notional_krw" not in payload
            and not bool(position.has_executable_exposure)
        ):
            payload["total_effective_exposure_notional_krw"] = 0.0
        execution_intent = decision.execution_intent
        if execution_intent is not None and hasattr(execution_intent, "as_dict"):
            payload["execution_intent"] = execution_intent.as_dict()
            payload["execution_intent_authority"] = "non_authoritative_strategy_hint"
        if self.target.portfolio_target is not None:
            target_payload = self.target.portfolio_target.as_dict()
            payload.update(
                {
                    "portfolio_target": target_payload,
                    "portfolio_target_present": True,
                    "portfolio_target_authoritative": bool(
                        self.target.portfolio_target.authoritative
                    ),
                    "portfolio_target_hash": self.target.portfolio_target_hash
                    or self.target.portfolio_target.content_hash(),
                    "allocation_decision_hash": self.target.allocation_decision_hash,
                    "allocator_config_hash": self.target.allocator_config_hash,
                    "strategy_contribution_hash": self.target.strategy_contribution_hash,
                    "allocator_policy": (
                        f"{self.target.portfolio_target.allocator_policy_name}:"
                        f"{self.target.portfolio_target.allocator_policy_version}"
                    ),
                    "allocator_reason": self.target.portfolio_target.reason,
                    "allocation_conflict_count": int(
                        self.target.portfolio_target.conflict_resolution.get("conflict_count") or 0
                    ),
                    "allocation_primary_block_reason": self.target.portfolio_target.fail_closed_reason,
                }
            )
        else:
            payload.update(
                {
                    "portfolio_target_present": False,
                    "portfolio_target_authoritative": False,
                    "portfolio_target_hash": self.target.portfolio_target_hash,
                    "allocation_decision_hash": self.target.allocation_decision_hash,
                    "allocator_config_hash": self.target.allocator_config_hash,
                    "strategy_contribution_hash": self.target.strategy_contribution_hash,
                    "allocator_policy": "",
                    "allocator_reason": "portfolio_target_missing",
                    "allocation_conflict_count": 0,
                    "allocation_primary_block_reason": "portfolio_target_missing",
                }
            )
        return payload


@dataclass(frozen=True)
class ResidualSellCandidate:
    qty: float
    notional: float | None
    source: str
    classes: tuple[str, ...]
    exchange_sellable: bool
    allowed_by_policy: bool
    requires_final_pre_submit_proof: bool


@dataclass(frozen=True)
class ResidualSellPreSubmitProof:
    passed: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class ExecutionSubmitPlan:
    side: str
    source: str
    authority: str
    final_action: str
    qty: float | None
    notional_krw: float | None
    target_exposure_krw: float | None
    current_effective_exposure_krw: float | None
    delta_krw: float | None
    submit_expected: bool
    pre_submit_proof_status: str
    block_reason: str
    idempotency_key: str | None
    extra_payload: dict[str, object] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        payload = {
            "side": self.side,
            "source": self.source,
            "authority": self.authority,
            "final_action": self.final_action,
            "qty": self.qty,
            "notional_krw": self.notional_krw,
            "target_exposure_krw": self.target_exposure_krw,
            "current_effective_exposure_krw": self.current_effective_exposure_krw,
            "delta_krw": self.delta_krw,
            "submit_expected": bool(self.submit_expected),
            "pre_submit_proof_status": self.pre_submit_proof_status,
            "block_reason": self.block_reason,
            "idempotency_key": self.idempotency_key,
        }
        payload.update(dict(self.extra_payload))
        return payload

    def content_hash(self) -> str:
        return execution_submit_plan_payload_hash(self.as_dict())

    def as_final_payload(self, *, extra: dict[str, object] | None = None) -> dict[str, object]:
        payload = self.as_dict()
        if extra:
            payload.update(extra)
        payload.setdefault("submit_plan_hash", self.content_hash())
        payload["schema_version"] = EXECUTION_SUBMIT_PLAN_SCHEMA_VERSION
        payload["authority_label"] = EXECUTION_SUBMIT_PLAN_AUTHORITY_LABEL
        payload["content_hash"] = execution_submit_plan_payload_hash(payload)
        validate_execution_submit_plan_payload(
            payload,
            field_name="execution_submit_plan",
            require_final_payload=True,
        )
        return payload

EXECUTION_SUBMIT_PLAN_VALID_SOURCES = frozenset(
    {
        "target_delta",
        "strategy_position",
        "residual_inventory",
        "research_backtest",
    }
)
EXECUTION_SUBMIT_PLAN_VALID_AUTHORITIES = frozenset(
    {
        "canonical_target_delta_sizing",
        "configured_strategy_order_size",
        "residual_inventory_policy",
        "residual_inventory_delta",
        "strategy_execution_intent",
        "research_compatibility_execution_intent",
        "target_position_delta",
    }
)
EXECUTION_SUBMIT_PLAN_REQUIRED_FIELDS = frozenset(
    {
        "side",
        "source",
        "authority",
        "final_action",
        "qty",
        "notional_krw",
        "target_exposure_krw",
        "current_effective_exposure_krw",
        "delta_krw",
        "submit_expected",
        "pre_submit_proof_status",
        "block_reason",
        "idempotency_key",
    }
)
EXECUTION_SUBMIT_PLAN_FINAL_REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "authority_label",
        "content_hash",
    }
)


def execution_submit_plan_payload_hash(plan: Mapping[str, object]) -> str:
    hash_input = {
        str(key): value
        for key, value in dict(plan).items()
        if key not in {"content_hash", "submit_plan_hash"}
    }
    return sha256_prefixed(hash_input)


def validate_execution_submit_plan_payload(
    plan: dict[str, object] | None,
    *,
    field_name: str,
    require_final_payload: bool = False,
) -> None:
    if plan is None:
        return
    missing = sorted(EXECUTION_SUBMIT_PLAN_REQUIRED_FIELDS.difference(plan))
    if missing:
        raise ValueError(f"{field_name}_schema_missing_fields:{','.join(missing)}")
    if require_final_payload:
        final_missing = sorted(EXECUTION_SUBMIT_PLAN_FINAL_REQUIRED_FIELDS.difference(plan))
        if final_missing:
            raise ValueError(f"{field_name}_schema_missing_fields:{','.join(final_missing)}")
        try:
            schema_version = int(plan.get("schema_version") or 0)
        except (TypeError, ValueError):
            schema_version = 0
        if schema_version != EXECUTION_SUBMIT_PLAN_SCHEMA_VERSION:
            raise ValueError(f"{field_name}_schema_invalid_version:{schema_version or 'missing'}")
        authority_label = str(plan.get("authority_label") or "")
        if authority_label != EXECUTION_SUBMIT_PLAN_AUTHORITY_LABEL:
            raise ValueError(f"{field_name}_schema_invalid_authority_label:{authority_label or 'missing'}")
        content_hash = str(plan.get("content_hash") or "")
        if not content_hash:
            raise ValueError(f"{field_name}_schema_missing_content_hash")
    side = str(plan.get("side") or "").upper()
    if side not in {"BUY", "SELL", "HOLD", "NONE"}:
        raise ValueError(f"{field_name}_schema_invalid_side:{side or 'missing'}")
    source = str(plan.get("source") or "").strip()
    if source not in EXECUTION_SUBMIT_PLAN_VALID_SOURCES:
        raise ValueError(f"{field_name}_schema_invalid_source:{source or 'missing'}")
    authority = str(plan.get("authority") or "").strip()
    if authority not in EXECUTION_SUBMIT_PLAN_VALID_AUTHORITIES:
        raise ValueError(f"{field_name}_schema_invalid_authority:{authority or 'missing'}")
    proof_status = str(plan.get("pre_submit_proof_status") or "")
    if proof_status not in {"passed", "failed", "not_required"}:
        raise ValueError(f"{field_name}_schema_invalid_pre_submit_proof_status:{proof_status}")
    if bool(plan.get("submit_expected")) and proof_status == "failed":
        raise ValueError(f"{field_name}_schema_submit_expected_with_failed_proof")
    block_reason = str(plan.get("block_reason") or "")
    if not block_reason:
        raise ValueError(f"{field_name}_schema_missing_block_reason")
    if require_final_payload:
        expected_hash = execution_submit_plan_payload_hash(plan)
        if content_hash != expected_hash:
            raise ValueError(f"{field_name}_schema_content_hash_mismatch")


def _log_live_submit_plan_block(
    *,
    reason: str,
    field_name: str,
    source: object | None = None,
    side: object | None = None,
) -> None:
    RUN_LOG.warning(
        format_log_kv(
            "[ORDER_SKIP] invalid execution submit plan",
            reason=str(reason),
            field_name=str(field_name),
            source=str(source or "-"),
            side=str(side or "-"),
            execution_engine=_execution_engine(),
        )
    )


def _block_live_submit_plan(
    *,
    reason: str,
    field_name: str,
    source: object | None = None,
    side: object | None = None,
) -> None:
    _log_live_submit_plan_block(
        reason=reason,
        field_name=field_name,
        source=source,
        side=side,
    )
    return None


def _live_submit_plan_schema_valid(
    plan: dict[str, object],
    *,
    field_name: str,
) -> bool:
    try:
        validate_execution_submit_plan_payload(
            plan,
            field_name=field_name,
            require_final_payload=_live_real_order_submit_plan_required(),
        )
    except ValueError as exc:
        _log_live_submit_plan_block(
            reason=str(exc),
            field_name=field_name,
            source=plan.get("source"),
            side=plan.get("side"),
        )
        return False
    return True


@dataclass(frozen=True)
class ExecutionDecisionSummary:
    raw_signal: str
    final_signal: str
    final_action: str
    submit_expected: bool
    pre_submit_proof_status: str
    block_reason: str
    strategy_sell_candidate: dict[str, object] | None
    residual_sell_candidate: dict[str, object] | None
    target_exposure_krw: float | None
    current_effective_exposure_krw: float | None
    tracked_residual_exposure_krw: float | None
    buy_delta_krw: float | None
    residual_live_sell_mode: str
    residual_buy_sizing_mode: str
    residual_submit_plan: ExecutionSubmitPlan | None
    buy_submit_plan: ExecutionSubmitPlan | None
    target_shadow_decision: dict[str, object] | None
    target_submit_plan: ExecutionSubmitPlan | None
    pre_trade_economics: dict[str, object] | None = None
    signal_flow: dict[str, object] | None = None

    def __post_init__(self) -> None:
        for field_name, plan in (
            ("residual_submit_plan", self.residual_submit_plan),
            ("buy_submit_plan", self.buy_submit_plan),
            ("target_submit_plan", self.target_submit_plan),
        ):
            if plan is not None and not isinstance(plan, ExecutionSubmitPlan):
                raise TypeError(f"{field_name}_must_be_execution_submit_plan")
        validate_execution_submit_plan_payload(
            self.residual_submit_plan.as_dict() if self.residual_submit_plan is not None else None,
            field_name="residual_submit_plan",
        )
        validate_execution_submit_plan_payload(
            self.buy_submit_plan.as_dict() if self.buy_submit_plan is not None else None,
            field_name="buy_submit_plan",
        )
        validate_execution_submit_plan_payload(
            self.target_submit_plan.as_dict() if self.target_submit_plan is not None else None,
            field_name="target_submit_plan",
        )

    def as_dict(self) -> dict[str, object]:
        signal_flow = None if self.signal_flow is None else dict(self.signal_flow)
        actual_primary_block_layer = (
            "none" if signal_flow is None else signal_flow.get("primary_block_layer") or "none"
        )
        actual_primary_block_reason = (
            "none" if signal_flow is None else signal_flow.get("primary_block_reason") or "none"
        )
        if (
            actual_primary_block_layer == "none"
            and not bool(self.submit_expected)
            and str(self.final_signal).upper() == "HOLD"
            and str(self.final_action).upper() in {"HOLD", "STRATEGY_HOLD", "LIVE_DRY_RUN_NO_SUBMIT"}
        ):
            actual_primary_block_layer = "strategy_signal_absent"
            actual_primary_block_reason = "raw_hold_no_entry_or_exit_signal"
        return {
            "execution_engine": _execution_engine(),
            "raw_signal": self.raw_signal,
            "final_signal": self.final_signal,
            "final_action": self.final_action,
            "submit_expected": bool(self.submit_expected),
            "pre_submit_proof_status": self.pre_submit_proof_status,
            "block_reason": self.block_reason,
            "strategy_sell_candidate": (
                None if self.strategy_sell_candidate is None else dict(self.strategy_sell_candidate)
            ),
            "residual_sell_candidate": (
                None if self.residual_sell_candidate is None else dict(self.residual_sell_candidate)
            ),
            "target_exposure_krw": self.target_exposure_krw,
            "current_effective_exposure_krw": self.current_effective_exposure_krw,
            "tracked_residual_exposure_krw": self.tracked_residual_exposure_krw,
            "buy_delta_krw": self.buy_delta_krw,
            "residual_live_sell_mode": self.residual_live_sell_mode,
            "residual_buy_sizing_mode": self.residual_buy_sizing_mode,
            "residual_submit_plan": (
                None if self.residual_submit_plan is None else _submit_plan_payload(self.residual_submit_plan)
            ),
            "buy_submit_plan": None if self.buy_submit_plan is None else _submit_plan_payload(self.buy_submit_plan),
            "target_shadow_decision": (
                None if self.target_shadow_decision is None else dict(self.target_shadow_decision)
            ),
            "target_submit_plan": (
                None if self.target_submit_plan is None else _submit_plan_payload(self.target_submit_plan)
            ),
            "pre_trade_economics": (
                None if self.pre_trade_economics is None else dict(self.pre_trade_economics)
            ),
            "signal_flow": signal_flow,
            "actual_primary_block_layer": actual_primary_block_layer,
            "actual_primary_block_reason": actual_primary_block_reason,
        }

    def typed_target_submit_plan(self) -> ExecutionSubmitPlan | None:
        return _typed_submit_plan(self.target_submit_plan)

    def typed_residual_submit_plan(self) -> ExecutionSubmitPlan | None:
        return _typed_submit_plan(self.residual_submit_plan)

    def typed_buy_submit_plan(self) -> ExecutionSubmitPlan | None:
        return _typed_submit_plan(self.buy_submit_plan)


def _submit_plan_payload(
    plan: ExecutionSubmitPlan | None,
) -> dict[str, object] | None:
    if plan is None:
        return None
    payload = plan.as_dict()
    payload["submit_plan_hash"] = plan.content_hash()
    return payload


def _typed_submit_plan(
    plan: ExecutionSubmitPlan | None,
) -> ExecutionSubmitPlan | None:
    return plan if isinstance(plan, ExecutionSubmitPlan) else None


def primary_execution_submit_plan(
    summary: ExecutionDecisionSummary | None,
) -> ExecutionSubmitPlan | None:
    if summary is None:
        return None
    if not all(
        callable(getattr(summary, name, None))
        for name in (
            "typed_target_submit_plan",
            "typed_residual_submit_plan",
            "typed_buy_submit_plan",
        )
    ):
        return None
    return (
        summary.typed_target_submit_plan()
        or summary.typed_residual_submit_plan()
        or summary.typed_buy_submit_plan()
    )


def _validate_submit_authority_before_executor(
    plan: Mapping[str, object],
    *,
    plan_kind: str,
    field_name: str,
) -> bool:
    decision = evaluate_submit_authority_policy(
        plan,
        settings_obj=settings,
        plan_kind=plan_kind,
    )
    if decision.allowed:
        return True
    _block_live_submit_plan(
        reason=decision.reason,
        field_name=field_name,
        source=decision.source,
        side=decision.side,
    )
    return False


def execution_submit_plan_invariant_error(
    plan: ExecutionSubmitPlan | Mapping[str, object] | None,
    *,
    compatibility_signal: object,
) -> str | None:
    if plan is None:
        return None
    payload = plan.as_dict() if isinstance(plan, ExecutionSubmitPlan) else dict(plan)
    if not bool(payload.get("submit_expected")):
        return None
    side = str(payload.get("side") or "").strip().upper()
    if side not in {"BUY", "SELL"}:
        return "execution_submit_plan_non_submittable_side"
    try:
        qty = float(payload.get("qty") or 0.0)
    except (TypeError, ValueError):
        return "execution_submit_plan_invalid_qty"
    if qty <= 0.0:
        return "execution_submit_plan_non_positive_qty"
    notional = payload.get("notional_krw")
    try:
        notional_value = None if notional is None else float(notional or 0.0)
    except (TypeError, ValueError):
        return "execution_submit_plan_invalid_notional"
    if notional_value is not None and notional_value <= 0.0:
        return "execution_submit_plan_non_positive_notional"
    if str(payload.get("block_reason") or "none") != "none":
        return "execution_submit_plan_block_reason_not_none"
    if not bool(payload.get("submit_expected")):
        return "execution_submit_plan_submit_not_expected"
    scalar_signal = str(compatibility_signal or "HOLD").strip().upper()
    if scalar_signal in {"BUY", "SELL"} and scalar_signal != side:
        return "execution_signal_submit_plan_mismatch"
    return None


def _with_submit_plan_extra(
    plan: ExecutionSubmitPlan,
    extra: dict[str, object],
) -> ExecutionSubmitPlan:
    merged = dict(plan.extra_payload)
    merged.update(extra)
    return replace(plan, extra_payload=merged)


def _live_real_order_typed_submit_plan_error(
    summary: ExecutionDecisionSummary,
) -> str | None:
    target_plan = _typed_submit_plan(summary.target_submit_plan)
    residual_plan = _typed_submit_plan(summary.residual_submit_plan)
    buy_plan = _typed_submit_plan(summary.buy_submit_plan)
    if target_plan is None and summary.target_submit_plan is not None:
        return "live_real_order_missing_typed_submit_plan:target_submit_plan"
    if residual_plan is None and summary.residual_submit_plan is not None:
        return "live_real_order_missing_typed_submit_plan:residual_submit_plan"
    if buy_plan is None and summary.buy_submit_plan is not None:
        return "live_real_order_missing_typed_submit_plan:buy_submit_plan"
    if target_plan is None and residual_plan is None and buy_plan is None:
        return "live_real_order_missing_typed_submit_plan"
    return None


def _request_execution_decision_payload(
    request: TypedExecutionRequest,
) -> tuple[dict[str, object] | None, str | None]:
    observability_context = _request_observability_payload(request)
    raw_execution_decision = (
        observability_context.get("execution_decision")
        if isinstance(observability_context, dict)
        else None
    )
    if raw_execution_decision is not None and not isinstance(raw_execution_decision, dict):
        return None, "execution_decision_schema_not_object"
    if (
        _live_real_order_submit_plan_required()
        and isinstance(raw_execution_decision, dict)
        and any(
            isinstance(raw_execution_decision.get(field), dict)
            for field in ("target_submit_plan", "residual_submit_plan", "buy_submit_plan")
        )
        and request.execution_decision_summary is None
        and getattr(request.execution_plan_bundle, "summary", None) is None
    ):
        return None, "live_real_order_dict_only_execution_decision_not_authority"
    typed_summary, typed_summary_error = require_typed_execution_decision_summary_for_live_real_order(
        request
    )
    if typed_summary_error is not None:
        return None, typed_summary_error
    typed_payload = typed_summary.as_dict() if typed_summary is not None else None

    explicit_non_authoritative_observability = isinstance(
        request.observability_payload,
        ExecutionObservabilityPayload,
    )
    if typed_payload is not None and raw_execution_decision is not None and not explicit_non_authoritative_observability:
        raw_payload = dict(raw_execution_decision)
        if typed_payload != raw_payload:
            return None, "execution_decision_summary_context_mismatch"
        return raw_payload, None
    if typed_payload is not None:
        return typed_payload, None
    if isinstance(raw_execution_decision, dict):
        if _live_real_order_submit_plan_required():
            return None, "live_real_order_dict_only_execution_decision_not_authority"
        return dict(raw_execution_decision), None
    return None, None


def _request_observability_payload(request: TypedExecutionRequest) -> dict[str, object] | None:
    payload = (
        request.observability_payload
        if request.observability_payload is not None
        else getattr(request, "observability_context", None)
        if getattr(request, "observability_context", None) is not None
        else getattr(request, "decision_context", None)
    )
    if isinstance(payload, ExecutionObservabilityPayload):
        return payload.as_dict()
    return payload


class SignalExecutionService(Protocol):
    def execute(self, request: TypedExecutionRequest) -> dict | None: ...


def paper_execute(
    signal: str,
    ts: int,
    market_price: float,
    *,
    strategy_name: str | None = None,
    decision_id: int | None = None,
    decision_reason: str | None = None,
    exit_rule_name: str | None = None,
) -> dict | None:
    from .broker.paper import paper_execute as _paper_execute

    return _paper_execute(
        signal,
        ts,
        market_price,
        strategy_name=strategy_name,
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
    )


def live_execute_signal(
    broker: "Broker",
    signal: str,
    ts: int,
    market_price: float,
    *,
    strategy_name: str | None = None,
    decision_id: int | None = None,
    decision_reason: str | None = None,
    exit_rule_name: str | None = None,
    execution_submit_plan: dict[str, object] | None = None,
) -> dict | None:
    from .broker.live import live_execute_signal as _live_execute_signal

    return _live_execute_signal(
        broker,
        signal,
        ts,
        market_price,
        strategy_name=strategy_name,
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
        execution_submit_plan=execution_submit_plan,
    )


def _residual_live_sell_mode() -> str:
    mode = str(getattr(settings, "RESIDUAL_LIVE_SELL_MODE", "telemetry") or "telemetry").strip().lower()
    return mode if mode in {"telemetry", "dry_run", "enabled"} else "telemetry"


def _residual_buy_sizing_mode() -> str:
    mode = str(getattr(settings, "RESIDUAL_BUY_SIZING_MODE", "telemetry") or "telemetry").strip().lower()
    return mode if mode in {"off", "telemetry", "delta"} else "telemetry"


def _execution_engine() -> str:
    engine = str(getattr(settings, "EXECUTION_ENGINE", "lot_native") or "lot_native").strip().lower()
    return engine if engine in {"lot_native", "target_delta"} else "lot_native"


def _live_real_order_performance_gate_applies() -> bool:
    return bool(
        str(getattr(settings, "MODE", "") or "").strip().lower() == "live"
        and bool(getattr(settings, "LIVE_REAL_ORDER_ARMED", False))
        and not bool(getattr(settings, "LIVE_DRY_RUN", True))
    )


def _live_real_order_submit_plan_required() -> bool:
    return bool(
        str(getattr(settings, "MODE", "") or "").strip().lower() == "live"
        and bool(getattr(settings, "LIVE_REAL_ORDER_ARMED", False))
        and not bool(getattr(settings, "LIVE_DRY_RUN", True))
    )


def require_typed_execution_decision_summary_for_live_real_order(
    request: TypedExecutionRequest,
) -> tuple[ExecutionDecisionSummary | None, str | None]:
    bundle_summary = getattr(request.execution_plan_bundle, "summary", None)
    if (
        request.execution_decision_summary is not None
        and bundle_summary is not None
        and isinstance(request.execution_decision_summary, ExecutionDecisionSummary)
        and isinstance(bundle_summary, ExecutionDecisionSummary)
        and request.execution_decision_summary.as_dict() != bundle_summary.as_dict()
    ):
        return None, "execution_decision_summary_bundle_mismatch"
    typed_summary = request.execution_decision_summary or bundle_summary
    if not _live_real_order_submit_plan_required():
        return typed_summary, None
    if typed_summary is None:
        return None, "live_real_order_missing_typed_execution_summary"
    if not isinstance(typed_summary, ExecutionDecisionSummary):
        return None, "live_real_order_invalid_typed_execution_summary"
    submit_plan_error = _live_real_order_typed_submit_plan_error(typed_summary)
    if submit_plan_error is not None:
        return None, submit_plan_error
    bundle_plan = getattr(request.execution_plan_bundle, "submit_plan", None)
    if bundle_plan is not None and not isinstance(bundle_plan, ExecutionSubmitPlan):
        return None, "live_real_order_invalid_execution_plan_bundle_submit_plan"
    summary_plan = primary_execution_submit_plan(typed_summary)
    if (
        isinstance(bundle_plan, ExecutionSubmitPlan)
        and summary_plan is not None
        and bundle_plan.as_dict() != summary_plan.as_dict()
    ):
        return None, "execution_submit_plan_bundle_summary_mismatch"
    return typed_summary, None


def _strategy_performance_gate_payload(raw_gate: object | None) -> dict[str, object] | None:
    if raw_gate is None:
        return None
    if isinstance(raw_gate, dict):
        return dict(raw_gate)
    as_dict = getattr(raw_gate, "as_dict", None)
    if callable(as_dict):
        payload = as_dict()
        return dict(payload) if isinstance(payload, dict) else None
    return None


def _strategy_performance_gate_fields(raw_gate: object | None) -> dict[str, object]:
    payload = _strategy_performance_gate_payload(raw_gate)
    if not payload:
        return {}
    summary = _dict_value(payload.get("summary"))
    blocked = bool(payload.get("blocked") or not bool(payload.get("allowed", True)))
    enabled = bool(payload.get("enabled", True))
    enforced = bool(blocked and enabled and _live_real_order_performance_gate_applies())
    status = "blocked" if blocked and enabled else "allowed" if enabled else "disabled"
    return {
        "strategy_performance_gate": payload,
        "strategy_performance_gate_status": status,
        "strategy_performance_gate_blocked": blocked,
        "strategy_performance_gate_enforced": enforced,
        "strategy_performance_gate_would_block_if_armed": bool(blocked and enabled),
        "strategy_performance_gate_reason_code": payload.get("reason_code"),
        "strategy_performance_gate_reason": payload.get("reason"),
        "strategy_performance_gate_sample_count": int(summary.get("sample_count") or 0),
        "strategy_performance_gate_expectancy_per_trade": float(summary.get("expectancy_per_trade") or 0.0),
        "strategy_performance_gate_net_pnl": float(summary.get("net_pnl") or 0.0),
        "strategy_performance_gate_profit_factor": summary.get("profit_factor"),
        "strategy_performance_gate_recommended_next_action": payload.get("recommended_next_action"),
        "recommended_next_action": payload.get("recommended_next_action"),
    }


def _target_delta_buy_blocked_by_performance_gate(raw_gate: object | None, *, side: str) -> bool:
    if str(side or "").upper() != "BUY":
        return False
    if not _live_real_order_performance_gate_applies():
        return False
    payload = _strategy_performance_gate_payload(raw_gate)
    return bool(payload and payload.get("enabled", True) and not bool(payload.get("allowed", True)))


def _cost_edge_context(decision_context: dict[str, object]) -> dict[str, object]:
    filters = decision_context.get("filters")
    if not isinstance(filters, dict):
        return {}
    cost_edge = filters.get("cost_edge")
    return dict(cost_edge) if isinstance(cost_edge, dict) else {}


def _build_buy_pre_trade_economics(
    *,
    decision_context: dict[str, object],
    plan: dict[str, object] | None,
    side: str,
    source: str,
) -> dict[str, object] | None:
    if str(side).strip().upper() != "BUY" or not isinstance(plan, dict):
        return None
    cost_edge = _cost_edge_context(decision_context)
    if not cost_edge:
        return None
    order_krw = plan.get("notional_krw", plan.get("target_final_submitted_notional_krw", plan.get("delta_krw")))
    snapshot = build_pre_trade_economics_snapshot(
        side="BUY",
        order_krw=None if order_krw is None else float(order_krw or 0.0),
        expected_edge_ratio=float(cost_edge.get("value", cost_edge.get("expected_edge_ratio", 0.0)) or 0.0),
        required_edge_ratio=float(cost_edge.get("threshold", cost_edge.get("required_edge_ratio", 0.0)) or 0.0),
        roundtrip_fee_ratio=float(cost_edge.get("roundtrip_fee_ratio", 0.0) or 0.0),
        slippage_ratio=float(cost_edge.get("slippage_ratio", 0.0) or 0.0),
        buffer_ratio=float(cost_edge.get("buffer_ratio", 0.0) or 0.0),
        min_net_edge_krw=float(getattr(settings, "MIN_NET_EDGE_KRW", 0.0) or 0.0),
        min_margin_after_cost_ratio=float(getattr(settings, "MIN_MARGIN_AFTER_COST_RATIO", 0.0) or 0.0),
        blocking_enabled=bool(getattr(settings, "PRE_TRADE_ECONOMICS_BLOCKING_ENABLED", False)),
        source=source,
    )
    return snapshot.as_dict()


def _execution_contract_reasons(
    *,
    target_or_buy_plan: dict[str, object] | None,
    pre_trade_economics: dict[str, object] | None,
) -> list[tuple[str, str]]:
    reasons: list[tuple[str, str]] = []
    if isinstance(pre_trade_economics, dict) and bool(pre_trade_economics.get("blocking_enabled")) and not bool(
        pre_trade_economics.get("meaningful_edge", True)
    ):
        reasons.append(("pre_trade_economics", str(pre_trade_economics.get("reason") or "net_edge_below_minimum")))
    if isinstance(target_or_buy_plan, dict):
        block_reason = str(target_or_buy_plan.get("block_reason") or "none")
        final_action = str(target_or_buy_plan.get("final_action") or "")
        submit_expected = bool(target_or_buy_plan.get("submit_expected"))
        if not submit_expected and block_reason not in {"", "none", "residual_buy_sizing_mode_telemetry"}:
            if "PERFORMANCE" in final_action or block_reason.startswith("STRATEGY_PERFORMANCE"):
                reasons.append(("performance_gate", block_reason))
            else:
                reasons.append(("execution_order_rule", block_reason))
    return reasons


def _residual_intent_ts(payload: dict[str, object]) -> int:
    for key in ("ts", "candle_ts", "signal_ts", "decision_ts"):
        try:
            value = payload.get(key)
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            continue
    return 0


def record_harmless_dust_exit_suppression(**kwargs) -> bool:
    from .broker.live import record_harmless_dust_exit_suppression as _record_harmless_dust_exit_suppression

    return _record_harmless_dust_exit_suppression(**kwargs)


def _dict_value(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, dict) else {}


def build_residual_sell_candidate(decision_context: dict[str, object] | None) -> ResidualSellCandidate | None:
    if not isinstance(decision_context, dict):
        return None
    residual_mode = str(decision_context.get("residual_inventory_mode") or "block")
    residual_state = str(decision_context.get("residual_inventory_state") or "")
    residual_inventory = _dict_value(decision_context.get("residual_inventory"))
    residual_candidate = _dict_value(decision_context.get("residual_sell_candidate"))
    if residual_mode != "track" or residual_state != "RESIDUAL_INVENTORY_TRACKED":
        return None
    if residual_candidate:
        return ResidualSellCandidate(
            qty=float(residual_candidate.get("qty") or 0.0),
            notional=(
                None if residual_candidate.get("notional") is None else float(residual_candidate.get("notional") or 0.0)
            ),
            source=str(residual_candidate.get("source") or "residual_inventory"),
            classes=tuple(str(item) for item in (residual_candidate.get("classes") or [])),
            exchange_sellable=bool(residual_candidate.get("exchange_sellable")),
            allowed_by_policy=bool(residual_candidate.get("allowed_by_policy")),
            requires_final_pre_submit_proof=bool(residual_candidate.get("requires_final_pre_submit_proof")),
        )
    if not bool(residual_inventory.get("exchange_sellable")):
        return None
    qty = float(residual_inventory.get("residual_qty") or 0.0)
    if qty <= 1e-12:
        return None
    return ResidualSellCandidate(
        qty=qty,
        notional=(
            None
            if residual_inventory.get("residual_notional_krw") is None
            else float(residual_inventory.get("residual_notional_krw") or 0.0)
        ),
        source="residual_inventory",
        classes=tuple(str(item) for item in (residual_inventory.get("residual_classes") or [])),
        exchange_sellable=True,
        allowed_by_policy=True,
        requires_final_pre_submit_proof=True,
    )


def build_residual_sell_presubmit_proof(decision_context: dict[str, object] | None) -> ResidualSellPreSubmitProof:
    reasons: list[str] = []
    if not isinstance(decision_context, dict):
        return ResidualSellPreSubmitProof(passed=False, reasons=("missing_decision_context",))
    candidate = build_residual_sell_candidate(decision_context)
    if candidate is None:
        reasons.append("missing_residual_sell_candidate")
    else:
        if not bool(candidate.allowed_by_policy):
            reasons.append("candidate_policy_blocked")
        if not bool(candidate.requires_final_pre_submit_proof):
            reasons.append("candidate_final_pre_submit_proof_not_required")
    if not bool(decision_context.get("residual_inventory_policy_allows_sell")):
        reasons.append("residual_sell_policy_blocked")
    if not bool(decision_context.get("projection_converged")):
        reasons.append("projection_not_converged")
    projection = _dict_value(decision_context.get("projection_convergence"))
    if projection and not bool(projection.get("converged")):
        reasons.append("projection_not_converged")
    if not bool(decision_context.get("accounting_projection_ok")):
        reasons.append(
            "missing_accounting_projection_ok"
            if "accounting_projection_ok" not in decision_context
            else "accounting_projection_not_ok"
        )
    if int(decision_context.get("open_order_count") or 0) > 0:
        reasons.append("open_order_count_nonzero")
    if int(decision_context.get("unresolved_open_order_count") or 0) > 0:
        reasons.append("unresolved_open_order_count_nonzero")
    if int(decision_context.get("recovery_required_count") or 0) > 0:
        reasons.append("recovery_required_count_nonzero")
    if int(decision_context.get("submit_unknown_count") or 0) > 0:
        reasons.append("submit_unknown_count_nonzero")
    broker_evidence = _dict_value(decision_context.get("broker_position_evidence"))
    locked_qty = (
        decision_context.get("locked_qty")
        if "locked_qty" in decision_context
        else decision_context.get("residual_proof_locked_qty", broker_evidence.get("asset_locked"))
    )
    if locked_qty is None:
        reasons.append("missing_locked_qty")
    elif float(locked_qty or 0.0) > 1e-12:
        reasons.append("locked_qty_nonzero")
    if bool(decision_context.get("active_fee_accounting_blocker")):
        reasons.append("active_fee_accounting_blocker")
    if not bool(broker_evidence.get("broker_qty_known")):
        reasons.append("broker_qty_unknown")
    if bool(broker_evidence.get("balance_source_stale")):
        reasons.append("broker_evidence_stale")
    if candidate is not None and float(broker_evidence.get("broker_qty") or 0.0) + 1e-12 < float(candidate.qty):
        reasons.append("broker_qty_below_candidate_qty")
    min_qty = decision_context.get("min_qty", decision_context.get("residual_proof_min_qty"))
    min_notional = decision_context.get(
        "min_notional_krw", decision_context.get("residual_proof_min_notional_krw")
    )
    if min_qty is None:
        reasons.append("missing_min_qty")
    elif candidate is not None and float(candidate.qty) + 1e-12 < float(min_qty):
        reasons.append("qty_below_min_qty")
    if min_notional is None:
        reasons.append("missing_min_notional")
    elif candidate is not None and (
        candidate.notional is None or float(candidate.notional) + 1e-9 < float(min_notional)
    ):
        reasons.append("notional_below_min_notional")
    if not str(decision_context.get("idempotency_scope") or "").strip():
        reasons.append("missing_idempotency_scope")
    return ResidualSellPreSubmitProof(passed=not reasons, reasons=tuple(dict.fromkeys(reasons)))


def _first_block_reason(*values: object, default: str = "none") -> str:
    for value in values:
        text = str(value or "").strip()
        if text and text.lower() != "none":
            return text
    return default


def _strategy_sell_candidate(decision_context: dict[str, object]) -> dict[str, object] | None:
    exposure = resolve_canonical_position_exposure_snapshot(decision_context)
    if int(exposure.sellable_executable_lot_count) <= 0 or not bool(exposure.exit_allowed):
        return None
    return {
        "source": "lot_native_strategy_position",
        "authority": "position_state.normalized_exposure.sellable_executable_lot_count",
        "sellable_executable_lot_count": int(exposure.sellable_executable_lot_count),
        "sellable_executable_qty": float(exposure.sellable_executable_qty),
    }


def _residual_block_reason(
    *,
    decision_context: dict[str, object],
    proof: ResidualSellPreSubmitProof | None,
) -> str:
    if proof is not None and proof.reasons:
        return str(proof.reasons[0])
    residual_inventory = _dict_value(decision_context.get("residual_inventory"))
    classes = {str(item) for item in (residual_inventory.get("residual_classes") or [])}
    if not bool(residual_inventory.get("exchange_sellable")):
        if "TRUE_DUST" in classes:
            return "below_min_qty_or_min_notional"
        return "residual_not_exchange_sellable"
    return _first_block_reason(
        decision_context.get("exit_block_reason"),
        decision_context.get("block_reason"),
        decision_context.get("reason"),
        default="residual_policy_blocked",
    )


def _portfolio_target_authority_error(
    *,
    portfolio_target: PortfolioTarget | None,
    portfolio_target_hash: str,
    required: bool,
) -> str | None:
    if not required:
        return None
    if portfolio_target is None:
        return "portfolio_target_missing"
    if not isinstance(portfolio_target, PortfolioTarget):
        return "portfolio_target_not_typed"
    if not bool(portfolio_target.authoritative):
        return str(portfolio_target.fail_closed_reason or "portfolio_target_not_authoritative")
    expected_hash = portfolio_target.content_hash()
    if not str(portfolio_target_hash or "").strip():
        return "portfolio_target_hash_missing"
    if str(portfolio_target_hash) != expected_hash:
        return "portfolio_target_hash_mismatch"
    if not str(portfolio_target.allocation_input_hash or "").strip():
        return "allocator_input_hash_missing"
    if not str(portfolio_target.strategy_contribution_hash or "").strip():
        return "strategy_contribution_hash_missing"
    if portfolio_target.target_exposure_krw is None:
        return "portfolio_target_exposure_missing"
    return None


def build_typed_execution_decision_summary(
    *,
    typed_input: TypedExecutionPlanningInput,
    strategy_performance_gate: object | None = None,
) -> ExecutionDecisionSummary:
    payload = typed_input.as_authority_payload()
    return _build_execution_decision_summary_from_authority_payload(
        authority_payload=payload,
        raw_signal=typed_input.strategy_decision.raw_signal,
        final_signal=typed_input.strategy_decision.final_signal,
        final_reason=typed_input.strategy_decision.final_reason,
        previous_target_exposure_krw=typed_input.target.previous_target_exposure_krw,
        portfolio_target=typed_input.target.portfolio_target,
        portfolio_target_hash=typed_input.target.portfolio_target_hash,
        portfolio_target_required=True,
        strategy_performance_gate=strategy_performance_gate,
    )


def build_execution_decision_summary(
    *,
    decision_context: dict[str, object] | None,
    readiness_payload: dict[str, object] | None = None,
    raw_signal: str | None = None,
    final_signal: str | None = None,
    final_reason: str | None = None,
    previous_target_exposure_krw: float | None = None,
    strategy_performance_gate: object | None = None,
) -> ExecutionDecisionSummary:
    """Compatibility wrapper for legacy dict callers.

    Runtime envelope planning must use ``build_typed_execution_decision_summary``.
    This wrapper is retained for older diagnostics/tests and makes the legacy
    authority boundary explicit.
    """
    payload: dict[str, object] = dict(decision_context or {})
    if isinstance(readiness_payload, dict):
        payload.update(
            ExecutionReadinessPlanningInput.from_payload(readiness_payload).as_payload()
        )
    return _build_execution_decision_summary_from_authority_payload(
        authority_payload=payload,
        raw_signal=raw_signal,
        final_signal=final_signal,
        final_reason=final_reason,
        previous_target_exposure_krw=previous_target_exposure_krw,
        strategy_performance_gate=strategy_performance_gate,
    )


def _build_execution_decision_summary_from_authority_payload(
    *,
    authority_payload: dict[str, object],
    raw_signal: str | None = None,
    final_signal: str | None = None,
    final_reason: str | None = None,
    previous_target_exposure_krw: float | None = None,
    portfolio_target: PortfolioTarget | None = None,
    portfolio_target_hash: str = "",
    portfolio_target_required: bool = False,
    strategy_performance_gate: object | None = None,
) -> ExecutionDecisionSummary:
    payload: dict[str, object] = dict(authority_payload)

    raw = str(raw_signal or payload.get("raw_signal") or payload.get("base_signal") or payload.get("signal") or "HOLD").upper()
    final = str(final_signal or payload.get("final_signal") or payload.get("signal") or "HOLD").upper()
    strategy_candidate = _strategy_sell_candidate(payload)
    residual_candidate = build_residual_sell_candidate(payload)
    residual_candidate_dict = None if residual_candidate is None else {
        "qty": float(residual_candidate.qty),
        "notional": residual_candidate.notional,
        "source": residual_candidate.source,
        "classes": list(residual_candidate.classes),
        "exchange_sellable": bool(residual_candidate.exchange_sellable),
        "allowed_by_policy": bool(residual_candidate.allowed_by_policy),
        "requires_final_pre_submit_proof": bool(residual_candidate.requires_final_pre_submit_proof),
    }

    target_exposure_krw = None
    current_effective_exposure_krw = None
    tracked_residual_exposure_krw = None
    buy_delta_krw = None
    if raw == "BUY":
        target_exposure_krw = max(0.0, float(getattr(settings, "MAX_ORDER_KRW", 0.0) or 0.0))
        execution_intent = _dict_value(payload.get("execution_intent"))
        if str(execution_intent.get("budget_model") or "") == "cash_fraction_capped_by_max_order_krw":
            cash_available = payload.get("cash_available")
            if cash_available is not None:
                target_exposure_krw = max(
                    0.0,
                    float(cash_available or 0.0)
                    * float(execution_intent.get("budget_fraction_of_cash") or 0.0),
                )
                max_budget = float(execution_intent.get("max_budget_krw") or 0.0)
                if max_budget > 0.0:
                    target_exposure_krw = min(target_exposure_krw, max_budget)
        current_effective_exposure_krw = (
            None
            if payload.get("total_effective_exposure_notional_krw") is None
            else max(0.0, float(payload.get("total_effective_exposure_notional_krw") or 0.0))
        )
        tracked_residual_exposure_krw = (
            None
            if payload.get("residual_inventory_notional_krw") is None
            else max(0.0, float(payload.get("residual_inventory_notional_krw") or 0.0))
        )
        if current_effective_exposure_krw is not None:
            buy_delta_krw = max(0.0, float(target_exposure_krw) - float(current_effective_exposure_krw))

    proof: ResidualSellPreSubmitProof | None = None
    if raw == "SELL" and residual_candidate is not None:
        proof = build_residual_sell_presubmit_proof(payload)

    residual_live_sell_mode = _residual_live_sell_mode()
    residual_buy_sizing_mode = _residual_buy_sizing_mode()
    residual_submit_plan: ExecutionSubmitPlan | None = None
    buy_submit_plan: ExecutionSubmitPlan | None = None
    target_shadow_decision: dict[str, object] | None = None
    target_submit_plan: ExecutionSubmitPlan | None = None
    pre_trade_economics: dict[str, object] | None = None
    execution_engine = _execution_engine()
    submit_authority_policy = submit_authority_policy_from_settings(settings)
    submit_authority_policy_hash = submit_authority_policy.content_hash()
    risk_decision = build_risk_decision_artifact(
        max_target_exposure_krw=getattr(portfolio_target, "target_exposure_krw", None),
        exposure_cap_source="portfolio_target",
        decision_context="execution_submit_plan",
    )
    risk_decision_hash = str(risk_decision["risk_decision_hash"])

    if bool(getattr(settings, "TARGET_EXECUTION_SHADOW", False)) or execution_engine == "target_delta":
        execution_order_rules = resolve_execution_order_rules(payload, market=str(settings.PAIR))
        target_authority_error = _portfolio_target_authority_error(
            portfolio_target=portfolio_target,
            portfolio_target_hash=portfolio_target_hash,
            required=execution_engine == "target_delta" and bool(portfolio_target_required),
        )
        authoritative_target_exposure_krw = (
            None
            if portfolio_target is None or target_authority_error is not None
            else portfolio_target.target_exposure_krw
        )
        target_decision = build_target_position_decision(
            raw_signal=raw,
            previous_target_exposure_krw=previous_target_exposure_krw,
            current_position_snapshot=None,
            readiness_payload=payload,
            order_rules=execution_order_rules.as_order_rules(),
            reference_price=payload.get("market_price", payload.get("last_close", payload.get("close"))),
            settings=TargetPositionSettings(
                execution_engine=execution_engine,
                shadow_enabled=execution_engine != "target_delta",
                target_exposure_krw=getattr(settings, "TARGET_EXPOSURE_KRW", None),
                max_order_krw=float(getattr(settings, "MAX_ORDER_KRW", 0.0) or 0.0),
                hold_policy=str(getattr(settings, "TARGET_HOLD_POLICY", "maintain_previous_target")),
            ),
            authoritative_target_exposure_krw=authoritative_target_exposure_krw,
        )
        target_shadow_decision = target_decision.as_dict()
        if target_authority_error is not None:
            target_shadow_decision.update(
                {
                    "portfolio_target_present": portfolio_target is not None,
                    "portfolio_target_authoritative": False
                    if portfolio_target is None
                    else bool(portfolio_target.authoritative),
                    "portfolio_target_hash": portfolio_target_hash,
                    "allocation_primary_block_reason": target_authority_error,
                }
            )
        if execution_engine == "target_delta":
            target_sizing = None
            target_sizing_dict: dict[str, object] | None = None
            if target_authority_error is None and target_decision.delta_side in {"BUY", "SELL"}:
                target_sizing = build_target_delta_execution_sizing(
                    pair=str(settings.PAIR),
                    side=str(target_decision.delta_side),
                    desired_qty=target_decision.submit_qty,
                    market_price=float(target_decision.reference_price or 0.0),
                    min_qty=target_decision.order_rule_min_qty,
                    qty_step=target_decision.order_rule_qty_step,
                    min_notional_krw=target_decision.order_rule_min_notional_krw,
                    max_qty_decimals=getattr(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 0),
                    authority_source="target_delta.desired_delta",
                )
                target_sizing_dict = target_sizing.as_dict()
            target_idempotency_key = None
            if target_sizing is not None and target_sizing.allowed:
                target_idempotency_key = build_order_intent_key(
                    symbol=str(settings.PAIR),
                    side=str(target_sizing.side),
                    strategy_context="target_delta",
                    intent_ts=_residual_intent_ts(payload),
                    intent_type="target_delta_rebalance",
                    qty=float(target_sizing.final_submitted_qty),
                )
            sizing_block_reason = (
                None
                if target_sizing is None or target_sizing.allowed
                else str(target_sizing.block_reason)
            )
            submit_allowed = bool(target_decision.would_submit and target_sizing is not None and target_sizing.allowed)
            if target_authority_error is not None:
                submit_allowed = False
            performance_gate_blocks_buy = bool(
                submit_allowed
                and _target_delta_buy_blocked_by_performance_gate(
                    strategy_performance_gate,
                    side=str(target_decision.delta_side),
                )
            )
            performance_gate_fields = _strategy_performance_gate_fields(strategy_performance_gate)
            if performance_gate_blocks_buy:
                submit_allowed = False
                sizing_block_reason = str(
                    performance_gate_fields.get("strategy_performance_gate_reason_code")
                    or "STRATEGY_PERFORMANCE_BLOCKED"
                )
            target_final_action = (
                "REBALANCE_TO_TARGET"
                if submit_allowed
                else (
                    "BLOCK_PORTFOLIO_TARGET_AUTHORITY"
                    if target_authority_error is not None
                    else (
                    "BLOCK_STRATEGY_PERFORMANCE_GATE"
                    if performance_gate_blocks_buy
                    else (
                        "HOLD_TARGET_TRUE_DUST"
                        if target_decision.block_reason == "delta_below_exchange_min"
                        else "BLOCK_TARGET_DELTA"
                    )
                    )
                )
            )
            target_block_reason = str(
                target_authority_error or sizing_block_reason or target_decision.block_reason
            )
            target_plan_extra = {
                "intent_type": "target_delta_rebalance",
                "strategy_context": "target_delta",
                "authority_source": "target_delta",
                "target_desired_qty": target_decision.submit_qty,
                "target_exchange_constrained_qty": (
                    None if target_sizing is None else target_sizing.exchange_constrained_qty
                ),
                "target_final_submitted_qty": (
                    None if target_sizing is None else target_sizing.final_submitted_qty
                ),
                "target_final_submitted_notional_krw": (
                    None if target_sizing is None else target_sizing.final_submitted_notional_krw
                ),
                "target_sizing": target_sizing_dict,
                "invariant_status": (
                    "not_required" if target_sizing is None else target_sizing.invariant_status
                ),
                "dust_policy": "no_delta" if target_sizing is None else target_sizing.dust_policy,
                "rejected_remainder": (
                    None if target_sizing is None else target_sizing.rejected_remainder
                ),
                "target_qty": target_decision.target_qty,
                "target_previous_exposure_krw": target_decision.previous_target_exposure_krw,
                "target_delta_qty": target_decision.delta_qty,
                "target_delta_side": target_decision.delta_side,
                "target_dust_classification": target_decision.dust_classification,
                "target_position_truth_state": target_decision.position_truth_state,
                "target_order_rule_min_qty": target_decision.order_rule_min_qty,
                "target_order_rule_min_notional_krw": target_decision.order_rule_min_notional_krw,
                "target_order_rule_qty_step": target_decision.order_rule_qty_step,
                "order_rule_authority": target_decision.order_rule_authority,
                "order_rule_authority_source": target_decision.order_rule_authority_source,
                "order_rule_authority_source_mode": target_decision.order_rule_authority_source_mode,
                "target_order_rule_min_qty_source": target_decision.order_rule_min_qty_source,
                "target_order_rule_min_notional_krw_source": target_decision.order_rule_min_notional_krw_source,
                "target_origin": target_decision.target_origin,
                "target_policy_action": target_decision.target_policy_action,
                "target_adoption_reason": target_decision.target_adoption_reason,
                "target_adopted_broker_qty": target_decision.target_adopted_broker_qty,
                "target_adopted_exposure_krw": target_decision.target_adopted_exposure_krw,
                "target_startup_policy_state": target_decision.target_startup_policy_state,
                "target_existing_state_present": target_decision.target_existing_state_present,
                "target_missing_state_resolution": target_decision.target_missing_state_resolution,
                "target_closeout_requested": target_decision.target_closeout_requested,
                "target_strategy_signal_source": target_decision.target_strategy_signal_source,
                "portfolio_target_present": bool(portfolio_target is not None),
                "portfolio_target_authoritative": (
                    False if portfolio_target is None else bool(portfolio_target.authoritative)
                ),
                "portfolio_target_hash": portfolio_target_hash,
                "allocation_decision_hash": str(payload.get("allocation_decision_hash") or ""),
                "allocator_config_hash": str(payload.get("allocator_config_hash") or ""),
                "strategy_contribution_hash": str(payload.get("strategy_contribution_hash") or ""),
                "allocator_policy": str(payload.get("allocator_policy") or ""),
                "allocator_reason": str(payload.get("allocator_reason") or ""),
                "allocation_conflict_count": int(payload.get("allocation_conflict_count") or 0),
                "allocation_primary_block_reason": str(
                    target_authority_error
                    or payload.get("allocation_primary_block_reason")
                    or "none"
                ),
                "submit_authority_mode": submit_authority_policy.submit_authority_mode,
                "submit_authority_policy_hash": submit_authority_policy_hash,
                "risk_decision": risk_decision,
                "risk_decision_hash": risk_decision_hash,
            }
            if performance_gate_fields and str(target_decision.delta_side) == "BUY":
                target_plan_extra.update(performance_gate_fields)
            target_plan = ExecutionSubmitPlan(
                side=str(target_decision.delta_side),
                source="target_delta",
                authority="canonical_target_delta_sizing",
                final_action=target_final_action,
                qty=(None if target_sizing is None else target_sizing.final_submitted_qty),
                notional_krw=(
                    None if target_sizing is None else target_sizing.final_submitted_notional_krw
                ),
                target_exposure_krw=target_decision.new_target_exposure_krw,
                current_effective_exposure_krw=target_decision.current_exposure_krw,
                delta_krw=target_decision.delta_notional_krw,
                submit_expected=submit_allowed,
                pre_submit_proof_status=("passed" if submit_allowed else "failed"),
                block_reason=target_block_reason,
                idempotency_key=target_idempotency_key,
            )
            pre_trade_plan = target_plan.as_final_payload(extra=target_plan_extra)
            pre_trade_economics = _build_buy_pre_trade_economics(
                decision_context=payload,
                plan=pre_trade_plan,
                side=str(target_decision.delta_side),
                source="target_submit_plan",
            )
            if pre_trade_economics is not None:
                target_plan_extra["pre_trade_economics"] = pre_trade_economics
                if bool(pre_trade_economics.get("blocking_enabled")) and not bool(
                    pre_trade_economics.get("meaningful_edge")
                ):
                    target_plan = ExecutionSubmitPlan(
                        side=target_plan.side,
                        source=target_plan.source,
                        authority=target_plan.authority,
                        final_action="BLOCK_PRE_TRADE_ECONOMICS",
                        qty=target_plan.qty,
                        notional_krw=target_plan.notional_krw,
                        target_exposure_krw=target_plan.target_exposure_krw,
                        current_effective_exposure_krw=target_plan.current_effective_exposure_krw,
                        delta_krw=target_plan.delta_krw,
                        submit_expected=False,
                        pre_submit_proof_status="failed",
                        block_reason=str(
                            pre_trade_economics.get("reason") or "net_edge_below_minimum"
                        ),
                        idempotency_key=target_plan.idempotency_key,
                    )
            target_submit_plan = _with_submit_plan_extra(target_plan, target_plan_extra)

    if execution_engine == "target_delta":
        if target_submit_plan is not None:
            target_submit_payload = target_submit_plan.as_dict()
            action = str(target_submit_payload["final_action"])
            submit_expected = bool(target_submit_payload["submit_expected"])
            proof_status = str(target_submit_payload["pre_submit_proof_status"])
            block_reason = str(target_submit_payload["block_reason"])
        else:
            action = "BLOCK_TARGET_DELTA"
            submit_expected = False
            proof_status = "failed"
            block_reason = "target_delta_decision_missing"
        contract_payload = dict(payload)
        if pre_trade_economics is not None:
            contract_payload["pre_trade_economics"] = pre_trade_economics
        contract_payload = apply_decision_contract(
            contract_payload,
            final_action=action,
            extra_block_reasons=_execution_contract_reasons(
                target_or_buy_plan=_submit_plan_payload(target_submit_plan),
                pre_trade_economics=pre_trade_economics,
            ),
        )
        return ExecutionDecisionSummary(
            raw_signal=raw,
            final_signal=final,
            final_action=action,
            submit_expected=submit_expected,
            pre_submit_proof_status=proof_status,
            block_reason=block_reason,
            strategy_sell_candidate=strategy_candidate,
            residual_sell_candidate=residual_candidate_dict,
            target_exposure_krw=(
                None if target_shadow_decision is None else target_shadow_decision.get("target_new_exposure_krw")
            ),
            current_effective_exposure_krw=(
                None if target_shadow_decision is None else target_shadow_decision.get("target_current_exposure_krw")
            ),
            tracked_residual_exposure_krw=tracked_residual_exposure_krw,
            buy_delta_krw=(
                None if target_shadow_decision is None else target_shadow_decision.get("target_delta_notional_krw")
            ),
            residual_live_sell_mode=residual_live_sell_mode,
            residual_buy_sizing_mode=residual_buy_sizing_mode,
            residual_submit_plan=None,
            buy_submit_plan=None,
            target_shadow_decision=target_shadow_decision,
            target_submit_plan=target_submit_plan,
            pre_trade_economics=pre_trade_economics,
            signal_flow=contract_payload.get("signal_flow") if isinstance(contract_payload.get("signal_flow"), dict) else None,
        )

    if raw == "BUY":
        if not bool(payload.get("residual_inventory_policy_allows_run", True)):
            action = "BLOCK_RECOVERY"
            submit_expected = False
            proof_status = "not_required"
            block_reason = _first_block_reason(payload.get("residual_inventory_state"), final_reason, default="recovery_blocked")
        elif (
            residual_buy_sizing_mode == "delta"
            and buy_delta_krw is not None
            and buy_delta_krw <= 0.0
        ):
            action = "HOLD_TARGET_ALREADY_COVERED"
            submit_expected = False
            proof_status = "not_required"
            block_reason = "tracked_residual_exposure_covers_target"
        elif (
            residual_buy_sizing_mode == "delta"
            and buy_delta_krw is not None
            and 0.0 < buy_delta_krw < float(payload.get("min_notional_krw", payload.get("residual_proof_min_notional_krw", 0.0)) or 0.0)
        ):
            action = "BLOCK_ORDER_RULE"
            submit_expected = False
            proof_status = "not_required"
            block_reason = "buy_delta_below_min_notional"
        elif final == "BUY":
            action = "ENTER_STRATEGY_POSITION"
            submit_expected = True
            proof_status = "not_required"
            block_reason = "none" if residual_buy_sizing_mode != "telemetry" else "residual_buy_sizing_mode_telemetry"
        elif buy_delta_krw is not None and buy_delta_krw <= 0.0:
            action = "HOLD_TARGET_ALREADY_COVERED"
            submit_expected = False
            proof_status = "not_required"
            block_reason = "tracked_residual_exposure_covers_target"
        else:
            action = "BLOCK_ORDER_RULE" if final == "HOLD" else "STRATEGY_HOLD"
            submit_expected = False
            proof_status = "not_required"
            block_reason = _first_block_reason(final_reason, payload.get("entry_block_reason"), payload.get("block_reason"))
        if buy_delta_krw is not None:
            delta_for_plan = (
                buy_delta_krw
                if residual_buy_sizing_mode == "delta"
                else target_exposure_krw
            )
            buy_submit_plan = ExecutionSubmitPlan(
                side="BUY",
                source="strategy_position",
                authority=(
                    "residual_inventory_delta"
                    if residual_buy_sizing_mode == "delta"
                    else "configured_strategy_order_size"
                ),
                final_action=action,
                qty=(None if delta_for_plan is None else float(delta_for_plan) / float(payload.get("market_price") or 1.0)),
                notional_krw=delta_for_plan,
                target_exposure_krw=target_exposure_krw,
                current_effective_exposure_krw=current_effective_exposure_krw,
                delta_krw=buy_delta_krw,
                submit_expected=submit_expected,
                pre_submit_proof_status=proof_status,
                block_reason=block_reason,
                idempotency_key=None,
            )
            pre_trade_economics = _build_buy_pre_trade_economics(
                decision_context=payload,
                plan=buy_submit_plan.as_dict(),
                side="BUY",
                source="buy_submit_plan",
            )
            if pre_trade_economics is not None:
                if bool(pre_trade_economics.get("blocking_enabled")) and not bool(
                    pre_trade_economics.get("meaningful_edge")
                ):
                    action = "BLOCK_PRE_TRADE_ECONOMICS"
                    submit_expected = False
                    proof_status = "failed"
                    block_reason = str(pre_trade_economics.get("reason") or "net_edge_below_minimum")
                    buy_submit_plan = replace(
                        buy_submit_plan,
                        final_action=action,
                        submit_expected=False,
                        pre_submit_proof_status=proof_status,
                        block_reason=block_reason,
                    )
                buy_submit_plan = _with_submit_plan_extra(
                    buy_submit_plan,
                    {"pre_trade_economics": pre_trade_economics},
                )
    elif raw == "SELL" or final == "SELL":
        if strategy_candidate is not None and final == "SELL":
            action = "EXIT_STRATEGY_POSITION"
            submit_expected = True
            proof_status = "not_required"
            block_reason = "none"
        elif residual_candidate is not None:
            action = "CLOSE_RESIDUAL_CANDIDATE" if proof is not None and proof.passed else "BLOCK_UNRESOLVED_RESIDUAL"
            proof_status = "passed" if proof is not None and proof.passed else "failed"
            if proof is not None and proof.passed:
                submit_expected = bool(
                    residual_live_sell_mode == "enabled"
                    and bool(getattr(settings, "LIVE_REAL_ORDER_ARMED", False))
                    and not bool(getattr(settings, "LIVE_DRY_RUN", True))
                )
                block_reason = (
                    "none"
                    if submit_expected
                    else (
                        "residual_live_sell_mode_telemetry"
                        if residual_live_sell_mode == "telemetry"
                        else (
                            "residual_live_sell_mode_dry_run"
                            if residual_live_sell_mode == "dry_run"
                            else "residual_live_sell_not_armed"
                        )
                    )
                )
            else:
                submit_expected = False
                block_reason = _residual_block_reason(decision_context=payload, proof=proof)
            residual_intent_key = build_order_intent_key(
                symbol=str(settings.PAIR),
                side="SELL",
                strategy_context="residual_inventory_policy",
                intent_ts=_residual_intent_ts(payload),
                intent_type="residual_close",
                qty=float(residual_candidate.qty),
            )
            residual_plan_extra = {
                "intent_type": "residual_close",
                "strategy_context": "residual_inventory_policy",
                "residual_inventory_policy_exception": True,
                "would_submit_pipeline": "standard",
                "would_intent_key": residual_intent_key,
                "would_client_order_id_shape": "live_<ts>_sell_<submit_attempt_id>",
                "would_order_type": "market",
                "would_source": "residual_inventory",
                "would_authority": "residual_inventory_policy",
                "would_submit_side": "SELL",
                "would_submit_qty": float(residual_candidate.qty),
                "submit_authority_mode": submit_authority_policy.submit_authority_mode,
                "submit_authority_policy_hash": submit_authority_policy_hash,
                "risk_decision": risk_decision,
                "risk_decision_hash": risk_decision_hash,
            }
            residual_submit_plan = ExecutionSubmitPlan(
                side="SELL",
                source="residual_inventory",
                authority="residual_inventory_policy",
                final_action=action,
                qty=float(residual_candidate.qty),
                notional_krw=residual_candidate.notional,
                target_exposure_krw=None,
                current_effective_exposure_krw=None,
                delta_krw=None,
                submit_expected=submit_expected,
                pre_submit_proof_status=proof_status,
                block_reason=block_reason,
                idempotency_key=residual_intent_key,
                extra_payload=residual_plan_extra,
            )
        elif str(payload.get("residual_inventory_state") or "") == "RESIDUAL_INVENTORY_UNRESOLVED":
            action = "BLOCK_UNRESOLVED_RESIDUAL"
            submit_expected = False
            proof_status = "failed"
            block_reason = _first_block_reason(payload.get("residual_inventory_state"), payload.get("exit_block_reason"))
        elif bool(payload.get("has_dust_only_remainder")):
            action = "HOLD_TRACKED_DUST"
            submit_expected = False
            proof_status = "not_required"
            block_reason = _residual_block_reason(decision_context=payload, proof=None)
        elif final == "HOLD":
            action = "STRATEGY_HOLD"
            submit_expected = False
            proof_status = "not_required"
            block_reason = _first_block_reason(final_reason, payload.get("exit_block_reason"), payload.get("block_reason"))
        else:
            action = "BLOCK_ORDER_RULE"
            submit_expected = False
            proof_status = "not_required"
            block_reason = _first_block_reason(final_reason, payload.get("exit_block_reason"), payload.get("block_reason"))
    else:
        action = "STRATEGY_HOLD"
        submit_expected = False
        proof_status = "not_required"
        block_reason = _first_block_reason(final_reason, payload.get("block_reason"))

    contract_payload = dict(payload)
    if pre_trade_economics is not None:
        contract_payload["pre_trade_economics"] = pre_trade_economics
    contract_payload = apply_decision_contract(
        contract_payload,
        final_action=action,
        extra_block_reasons=_execution_contract_reasons(
            target_or_buy_plan=_submit_plan_payload(target_submit_plan or buy_submit_plan),
            pre_trade_economics=pre_trade_economics,
        ),
    )
    return ExecutionDecisionSummary(
        raw_signal=raw,
        final_signal=final,
        final_action=action,
        submit_expected=submit_expected,
        pre_submit_proof_status=proof_status,
        block_reason=block_reason,
        strategy_sell_candidate=strategy_candidate,
        residual_sell_candidate=residual_candidate_dict,
        target_exposure_krw=target_exposure_krw,
        current_effective_exposure_krw=current_effective_exposure_krw,
        tracked_residual_exposure_krw=tracked_residual_exposure_krw,
        buy_delta_krw=buy_delta_krw,
        residual_live_sell_mode=residual_live_sell_mode,
        residual_buy_sizing_mode=residual_buy_sizing_mode,
        residual_submit_plan=residual_submit_plan,
        buy_submit_plan=buy_submit_plan,
        target_shadow_decision=target_shadow_decision,
        target_submit_plan=target_submit_plan,
        pre_trade_economics=pre_trade_economics,
        signal_flow=contract_payload.get("signal_flow") if isinstance(contract_payload.get("signal_flow"), dict) else None,
    )


def _canonical_harmless_dust_sell_preview(decision_context: dict[str, object] | None) -> dict[str, float | str] | None:
    if not isinstance(decision_context, dict):
        return None
    if build_residual_sell_candidate(decision_context) is not None:
        return None

    canonical_exposure = resolve_canonical_position_exposure_snapshot(decision_context)
    if bool(canonical_exposure.exit_allowed):
        return None
    if int(canonical_exposure.sellable_executable_lot_count) > 0:
        return None

    exit_block_reason = str(canonical_exposure.exit_block_reason or "").strip()
    if exit_block_reason not in {"dust_only_remainder", "no_executable_exit_lot"}:
        return None

    requested_qty = max(0.0, float(canonical_exposure.raw_total_asset_qty))
    if requested_qty <= 1e-12:
        return None

    return {
        "requested_qty": requested_qty,
        "normalized_qty": max(0.0, float(canonical_exposure.sellable_executable_qty)),
        "raw_total_asset_qty": requested_qty,
        "open_exposure_qty": max(0.0, float(canonical_exposure.open_exposure_qty)),
        "dust_tracking_qty": max(0.0, float(canonical_exposure.dust_tracking_qty)),
        "submit_qty_source": "position_state.normalized_exposure.sellable_executable_lot_count",
    }


def _paper_typed_submit_plan(
    request: TypedExecutionRequest,
) -> tuple[ExecutionSubmitPlan | None, str | None]:
    bundle = request.execution_plan_bundle
    bundle_present = bundle is not None
    bundle_plan = getattr(bundle, "submit_plan", None)
    bundle_summary = getattr(bundle, "summary", None)
    summary = request.execution_decision_summary or bundle_summary
    observability = _request_observability_payload(request)
    execution_plan_bundle_marker = (
        observability.get("execution_plan_bundle_present")
        if isinstance(observability, dict)
        else None
    )
    legacy_context_only = bool(
        isinstance(observability, dict)
        and str(execution_plan_bundle_marker).strip().lower() in {"0", "false", "none", ""}
        and str(observability.get("decision_authority_source") or "legacy_context") == "legacy_context"
        and not bool(observability.get("promotion_grade"))
        and summary is not None
        and getattr(summary, "target_submit_plan", None) is None
        and getattr(summary, "residual_submit_plan", None) is None
        and getattr(summary, "buy_submit_plan", None) is None
        and not bool(getattr(summary, "submit_expected", False))
    )
    typed_or_promotion_path = bool(
        bundle_present
        or (request.execution_decision_summary is not None and not legacy_context_only)
        or (isinstance(observability, dict) and bool(observability.get("execution_plan_bundle_present")))
        or (isinstance(observability, dict) and bool(observability.get("promotion_grade")))
    )
    if not typed_or_promotion_path:
        return None, None
    if bundle_present and bundle_plan is not None and not isinstance(bundle_plan, ExecutionSubmitPlan):
        return None, "paper_dict_only_submit_plan_not_authority"
    if summary is None:
        return None, "paper_missing_typed_execution_summary"
    if not isinstance(summary, ExecutionDecisionSummary):
        return None, "paper_invalid_typed_execution_summary"
    for field_name, candidate in (
        ("target_submit_plan", summary.target_submit_plan),
        ("residual_submit_plan", summary.residual_submit_plan),
        ("buy_submit_plan", summary.buy_submit_plan),
    ):
        if candidate is not None and not isinstance(candidate, ExecutionSubmitPlan):
            return None, f"paper_dict_only_submit_plan_not_authority:{field_name}"
    plan = bundle_plan if isinstance(bundle_plan, ExecutionSubmitPlan) else (
        summary.typed_target_submit_plan()
        or summary.typed_residual_submit_plan()
        or summary.typed_buy_submit_plan()
    )
    if plan is None:
        return None, "paper_missing_typed_submit_plan"
    if not bool(plan.submit_expected):
        return None, str(plan.block_reason or "paper_submit_plan_submit_not_expected")
    if str(plan.block_reason or "none") != "none":
        return None, str(plan.block_reason or "paper_submit_plan_blocked")
    invariant_error = execution_submit_plan_invariant_error(
        plan,
        compatibility_signal=request.signal,
    )
    if invariant_error is not None:
        return None, invariant_error
    if str(plan.side or "").upper() not in {"BUY", "SELL"}:
        return None, "paper_submit_plan_non_submittable_side"
    if plan.qty is None or float(plan.qty or 0.0) <= 0.0:
        return None, "paper_submit_plan_non_positive_qty"
    if plan.notional_krw is None or float(plan.notional_krw or 0.0) <= 0.0:
        return None, "paper_submit_plan_non_positive_notional"
    try:
        validate_execution_submit_plan_payload(plan.as_dict(), field_name="paper_submit_plan")
    except ValueError as exc:
        return None, str(exc)
    return plan, None


@dataclass(frozen=True)
class PaperSignalExecutionService:
    executor: Callable[..., dict | None]

    def execute(self, request: TypedExecutionRequest) -> dict | None:
        submit_plan, submit_plan_error = _paper_typed_submit_plan(request)
        if submit_plan_error is not None:
            RUN_LOG.warning(
                format_log_kv(
                    "[ORDER_SKIP] invalid paper execution submit plan",
                    reason=submit_plan_error,
                    signal=str(request.signal).upper(),
                    execution_engine=_execution_engine(),
                )
            )
            return None
        try:
            return self.executor(
                submit_plan.side if submit_plan is not None else request.signal,
                request.ts,
                request.market_price,
                strategy_name=request.strategy_name,
                decision_id=request.decision_id,
                decision_reason=request.decision_reason,
                exit_rule_name=request.exit_rule_name,
                execution_submit_plan=submit_plan,
            )
        except TypeError as exc:
            if "unexpected keyword argument" not in str(exc):
                raise
            if submit_plan is not None:
                RUN_LOG.warning(
                    format_log_kv(
                        "[ORDER_SKIP] paper executor missing typed submit plan support",
                        reason="paper_executor_missing_execution_submit_plan_support",
                        submit_plan_source=submit_plan.source,
                        submit_plan_authority=submit_plan.authority,
                        side=submit_plan.side,
                    )
                )
                return None
            return self.executor(request.signal, request.ts, request.market_price)


@dataclass(frozen=True)
class LiveSignalExecutionService:
    broker: "Broker"
    executor: Callable[..., dict | None]
    harmless_dust_recorder: Callable[..., bool]

    def record_harmless_dust_suppression_if_applicable(
        self,
        request: TypedExecutionRequest,
    ) -> bool:
        if request.signal != "SELL":
            return False
        harmless_dust_preview = _canonical_harmless_dust_sell_preview(
            getattr(request, "decision_context", None)
        )
        if harmless_dust_preview is None:
            return False
        suppression_conn = ensure_db()
        try:
            recorded = self.harmless_dust_recorder(
                conn=suppression_conn,
                state=runtime_state.snapshot(),
                signal=request.signal,
                side="SELL",
                requested_qty=float(harmless_dust_preview["requested_qty"]),
                market_price=float(request.market_price),
                normalized_qty=float(harmless_dust_preview["normalized_qty"]),
                strategy_name=request.strategy_name or settings.STRATEGY_NAME,
                decision_id=request.decision_id,
                decision_reason=request.decision_reason,
                exit_rule_name=request.exit_rule_name,
                submit_qty_source=str(harmless_dust_preview["submit_qty_source"]),
                position_state_source=str(harmless_dust_preview["submit_qty_source"]),
                raw_total_asset_qty=float(harmless_dust_preview["raw_total_asset_qty"]),
                open_exposure_qty=float(harmless_dust_preview["open_exposure_qty"]),
                dust_tracking_qty=float(harmless_dust_preview["dust_tracking_qty"]),
            )
            if recorded:
                suppression_conn.commit()
            return bool(recorded)
        finally:
            suppression_conn.close()

    def execute(self, request: TypedExecutionRequest) -> dict | None:
        submit_plan_required = _live_real_order_submit_plan_required()
        observability_context = _request_observability_payload(request)
        if observability_context is not None and not isinstance(observability_context, dict):
            field_name = (
                "decision_context"
                if request.observability_payload is None
                and getattr(request, "observability_context", None) is None
                and getattr(request, "decision_context", None) is observability_context
                else "observability_context"
            )
            _log_live_submit_plan_block(
                reason=f"{field_name}_schema_not_object",
                field_name=field_name,
            )
            return None
        decision_context = dict(observability_context or {})
        execution_decision, execution_decision_error = _request_execution_decision_payload(request)
        if execution_decision_error is not None:
            _log_live_submit_plan_block(
                reason=execution_decision_error,
                field_name="execution_decision",
            )
            return None
        if submit_plan_required and execution_decision is None:
            _log_live_submit_plan_block(
                reason="live_real_order_missing_execution_decision",
                field_name="execution_decision",
            )
            return None
        execution_decision = dict(execution_decision or {})
        typed_summary, typed_summary_error = require_typed_execution_decision_summary_for_live_real_order(
            request
        )
        if typed_summary_error is not None:
            _log_live_submit_plan_block(
                reason=typed_summary_error,
                field_name="execution_summary",
            )
            return None
        if (
            "target_submit_plan" in execution_decision
            and execution_decision.get("target_submit_plan") is not None
            and not isinstance(execution_decision.get("target_submit_plan"), dict)
        ):
            _log_live_submit_plan_block(
                reason="target_submit_plan_schema_not_object",
                field_name="target_submit_plan",
            )
            return None
        if (
            "residual_submit_plan" in execution_decision
            and execution_decision.get("residual_submit_plan") is not None
            and not isinstance(execution_decision.get("residual_submit_plan"), dict)
        ):
            _log_live_submit_plan_block(
                reason="residual_submit_plan_schema_not_object",
                field_name="residual_submit_plan",
            )
            return None
        if (
            "buy_submit_plan" in execution_decision
            and execution_decision.get("buy_submit_plan") is not None
            and not isinstance(execution_decision.get("buy_submit_plan"), dict)
        ):
            _log_live_submit_plan_block(
                reason="buy_submit_plan_schema_not_object",
                field_name="buy_submit_plan",
            )
            return None
        residual_plan = (
            dict(execution_decision.get("residual_submit_plan"))
            if isinstance(execution_decision.get("residual_submit_plan"), dict)
            else {}
        )
        target_plan = (
            dict(execution_decision.get("target_submit_plan"))
            if isinstance(execution_decision.get("target_submit_plan"), dict)
            else {}
        )
        buy_plan = (
            dict(execution_decision.get("buy_submit_plan"))
            if isinstance(execution_decision.get("buy_submit_plan"), dict)
            else {}
        )
        if typed_summary is not None:
            typed_target_plan = typed_summary.typed_target_submit_plan()
            typed_residual_plan = typed_summary.typed_residual_submit_plan()
            typed_buy_plan = typed_summary.typed_buy_submit_plan()
            try:
                if typed_target_plan is not None:
                    target_plan = typed_target_plan.as_final_payload()
                if typed_residual_plan is not None:
                    residual_plan = typed_residual_plan.as_final_payload()
                if typed_buy_plan is not None:
                    buy_plan = typed_buy_plan.as_final_payload()
            except ValueError as exc:
                _log_live_submit_plan_block(
                    reason=str(exc),
                    field_name="execution_submit_plan",
                    side=request.signal,
                )
                return None
        submit_authority_policy = submit_authority_policy_from_settings(settings)
        if submit_authority_policy.live_real_order_requires_target_delta and _execution_engine() != "target_delta":
            _log_live_submit_plan_block(
                reason="live_real_order_requires_execution_engine_target_delta",
                field_name="execution_engine",
                side=request.signal,
            )
            return None
        primary_plan = target_plan or residual_plan or buy_plan
        invariant_error = execution_submit_plan_invariant_error(
            primary_plan,
            compatibility_signal=request.signal,
        )
        if (
            invariant_error == "execution_signal_submit_plan_mismatch"
            and _execution_engine() == "target_delta"
            and str(settings.MODE).lower() == "live"
        ):
            _log_live_submit_plan_block(
                reason=invariant_error,
                field_name="execution_submit_plan",
                source=primary_plan.get("source") if isinstance(primary_plan, dict) else None,
                side=primary_plan.get("side") if isinstance(primary_plan, dict) else request.signal,
            )
            return None
        if submit_plan_required and not target_plan and not residual_plan and not buy_plan:
            _log_live_submit_plan_block(
                reason="live_real_order_missing_typed_submit_plan",
                field_name="execution_decision",
                source=execution_decision.get("source"),
                side=request.signal,
            )
            return None
        if target_plan and not _live_submit_plan_schema_valid(
            target_plan,
            field_name="target_submit_plan",
        ):
            return None
        if residual_plan and not _live_submit_plan_schema_valid(
            residual_plan,
            field_name="residual_submit_plan",
        ):
            return None
        if buy_plan and not _live_submit_plan_schema_valid(
            buy_plan,
            field_name="buy_submit_plan",
        ):
            return None
        if _execution_engine() == "target_delta" and str(settings.MODE).lower() != "live" and target_plan:
            plan_side = str(target_plan.get("side") or request.signal).upper()
            if not _validate_submit_authority_before_executor(
                target_plan,
                plan_kind="target",
                field_name="target_submit_plan",
            ):
                return None
            return self.executor(
                self.broker,
                plan_side,
                request.ts,
                request.market_price,
                strategy_name=request.strategy_name,
                decision_id=request.decision_id,
                decision_reason=request.decision_reason,
                exit_rule_name=request.exit_rule_name,
                execution_submit_plan=target_plan,
            )
        if _execution_engine() == "target_delta" and str(settings.MODE).lower() == "live":
            if not target_plan:
                if (
                    request.signal == "SELL"
                    and residual_plan
                    and str(residual_plan.get("source")) == "residual_inventory"
                ):
                    pass
                elif buy_plan:
                    if not _validate_submit_authority_before_executor(
                        buy_plan,
                        plan_kind="buy",
                        field_name="buy_submit_plan",
                    ):
                        return None
                else:
                    _block_live_submit_plan(
                        reason="target_delta_missing_target_submit_plan",
                        field_name="target_submit_plan",
                        source=execution_decision.get("source"),
                        side=request.signal,
                    )
                    return None
            if target_plan:
                if not bool(target_plan.get("portfolio_target_authoritative")):
                    _block_live_submit_plan(
                        reason="target_delta_missing_authoritative_portfolio_target",
                        field_name="target_submit_plan",
                        source=target_plan.get("source"),
                        side=target_plan.get("side"),
                    )
                    return None
                if not str(target_plan.get("portfolio_target_hash") or "").strip():
                    _block_live_submit_plan(
                        reason="target_delta_missing_portfolio_target_hash",
                        field_name="target_submit_plan",
                        source=target_plan.get("source"),
                        side=target_plan.get("side"),
                    )
                    return None
                if not str(target_plan.get("allocation_decision_hash") or "").strip():
                    _block_live_submit_plan(
                        reason="target_delta_missing_allocation_decision_hash",
                        field_name="target_submit_plan",
                        source=target_plan.get("source"),
                        side=target_plan.get("side"),
                    )
                    return None
                if not str(target_plan.get("strategy_contribution_hash") or "").strip():
                    _block_live_submit_plan(
                        reason="target_delta_missing_strategy_contribution_hash",
                        field_name="target_submit_plan",
                        source=target_plan.get("source"),
                        side=target_plan.get("side"),
                    )
                    return None
                if str(target_plan.get("source")) != "target_delta":
                    _block_live_submit_plan(
                        reason="target_delta_invalid_target_submit_plan_source",
                        field_name="target_submit_plan",
                        source=target_plan.get("source"),
                        side=target_plan.get("side"),
                    )
                    return None
                if str(target_plan.get("authority")) not in {
                    "canonical_target_delta_sizing",
                    "target_position_delta",
                }:
                    _block_live_submit_plan(
                        reason="target_delta_invalid_target_submit_plan_authority",
                        field_name="target_submit_plan",
                        source=target_plan.get("source"),
                        side=target_plan.get("side"),
                    )
                    return None
                if str(target_plan.get("block_reason") or "none") != "none":
                    _block_live_submit_plan(
                        reason="target_delta_blocked_submit_plan",
                        field_name="target_submit_plan",
                        source=target_plan.get("source"),
                        side=target_plan.get("side"),
                    )
                    return None
                if str(target_plan.get("pre_submit_proof_status") or "") != "passed":
                    _block_live_submit_plan(
                        reason="target_delta_pre_submit_proof_not_passed",
                        field_name="target_submit_plan",
                        source=target_plan.get("source"),
                        side=target_plan.get("side"),
                    )
                    return None
                if not bool(target_plan.get("submit_expected")):
                    _block_live_submit_plan(
                        reason="target_delta_submit_not_expected",
                        field_name="target_submit_plan",
                        source=target_plan.get("source"),
                        side=target_plan.get("side"),
                    )
                    return None
                plan_side = str(target_plan.get("side") or "").upper()
                if plan_side not in {"BUY", "SELL"}:
                    _block_live_submit_plan(
                        reason="target_delta_non_submittable_side",
                        field_name="target_submit_plan",
                        source=target_plan.get("source"),
                        side=target_plan.get("side"),
                    )
                    return None
                try:
                    plan_qty = float(target_plan.get("qty") or 0.0)
                except (TypeError, ValueError):
                    _block_live_submit_plan(
                        reason="target_delta_invalid_qty",
                        field_name="target_submit_plan",
                        source=target_plan.get("source"),
                        side=target_plan.get("side"),
                    )
                    return None
                if plan_qty <= 0.0:
                    _block_live_submit_plan(
                        reason="target_delta_non_positive_qty",
                        field_name="target_submit_plan",
                        source=target_plan.get("source"),
                        side=target_plan.get("side"),
                    )
                    return None
                try:
                    if not _live_submit_plan_schema_valid(
                        target_plan,
                        field_name="target_submit_plan",
                    ):
                        return None
                    if not _validate_submit_authority_before_executor(
                        target_plan,
                        plan_kind="target",
                        field_name="target_submit_plan",
                    ):
                        return None
                    return self.executor(
                        self.broker,
                        plan_side,
                        request.ts,
                        request.market_price,
                        strategy_name=request.strategy_name,
                        decision_id=request.decision_id,
                        decision_reason=request.decision_reason,
                        exit_rule_name=request.exit_rule_name,
                        execution_submit_plan=target_plan,
                    )
                except TypeError as exc:
                    if "unexpected keyword argument" not in str(exc):
                        raise
                    return {
                        "status": "blocked",
                        "reason": "executor_missing_execution_submit_plan_support",
                        "side": plan_side,
                        "source": "target_delta",
                        "authority": "target_position_delta",
                    }
            if not residual_plan:
                _block_live_submit_plan(
                    reason="target_delta_missing_target_submit_plan",
                    field_name="target_submit_plan",
                    source=execution_decision.get("source"),
                    side=request.signal,
                )
                return None
        if request.signal == "BUY" and buy_plan:
            if str(buy_plan.get("source")) != "strategy_position":
                _block_live_submit_plan(
                    reason="buy_submit_plan_invalid_source",
                    field_name="buy_submit_plan",
                    source=buy_plan.get("source"),
                    side=buy_plan.get("side"),
                )
                return None
            if str(buy_plan.get("authority")) not in {
                "configured_strategy_order_size",
                "residual_inventory_delta",
            }:
                _block_live_submit_plan(
                    reason="buy_submit_plan_invalid_authority",
                    field_name="buy_submit_plan",
                    source=buy_plan.get("source"),
                    side=buy_plan.get("side"),
                )
                return None
            if str(buy_plan.get("side") or "").upper() != "BUY":
                _block_live_submit_plan(
                    reason="buy_submit_plan_non_buy_side",
                    field_name="buy_submit_plan",
                    source=buy_plan.get("source"),
                    side=buy_plan.get("side"),
                )
                return None
            if str(buy_plan.get("block_reason") or "none") != "none":
                _block_live_submit_plan(
                    reason="buy_submit_plan_blocked",
                    field_name="buy_submit_plan",
                    source=buy_plan.get("source"),
                    side=buy_plan.get("side"),
                )
                return None
            if str(buy_plan.get("pre_submit_proof_status") or "") not in {"passed", "not_required"}:
                _block_live_submit_plan(
                    reason="buy_submit_plan_pre_submit_proof_not_compatible",
                    field_name="buy_submit_plan",
                    source=buy_plan.get("source"),
                    side=buy_plan.get("side"),
                )
                return None
            if not bool(buy_plan.get("submit_expected")):
                _block_live_submit_plan(
                    reason="buy_submit_plan_submit_not_expected",
                    field_name="buy_submit_plan",
                    source=buy_plan.get("source"),
                    side=buy_plan.get("side"),
                )
                return None
            try:
                plan_qty = float(buy_plan.get("qty") or 0.0)
                plan_notional = float(buy_plan.get("notional_krw") or 0.0)
            except (TypeError, ValueError):
                _block_live_submit_plan(
                    reason="buy_submit_plan_invalid_size",
                    field_name="buy_submit_plan",
                    source=buy_plan.get("source"),
                    side=buy_plan.get("side"),
                )
                return None
            if plan_qty <= 0.0 or plan_notional <= 0.0:
                _block_live_submit_plan(
                    reason="buy_submit_plan_non_positive_size",
                    field_name="buy_submit_plan",
                    source=buy_plan.get("source"),
                    side=buy_plan.get("side"),
                )
                return None
            try:
                if not _live_submit_plan_schema_valid(
                    buy_plan,
                    field_name="buy_submit_plan",
                ):
                    return None
                if not _validate_submit_authority_before_executor(
                    buy_plan,
                    plan_kind="buy",
                    field_name="buy_submit_plan",
                ):
                    return None
                return self.executor(
                    self.broker,
                    "BUY",
                    request.ts,
                    request.market_price,
                    strategy_name=request.strategy_name,
                    decision_id=request.decision_id,
                    decision_reason=request.decision_reason,
                    exit_rule_name=request.exit_rule_name,
                    execution_submit_plan=buy_plan,
                )
            except TypeError as exc:
                if "unexpected keyword argument" not in str(exc):
                    raise
                return {
                    "status": "blocked",
                    "reason": "executor_missing_execution_submit_plan_support",
                    "side": "BUY",
                    "source": "strategy_position",
                    "authority": str(buy_plan.get("authority") or "configured_strategy_order_size"),
                }
        if (
            request.signal == "SELL"
            and residual_plan
            and str(residual_plan.get("source")) == "residual_inventory"
        ):
            if str(residual_plan.get("block_reason") or "none") != "none":
                _block_live_submit_plan(
                    reason="residual_submit_plan_blocked",
                    field_name="residual_submit_plan",
                    source=residual_plan.get("source"),
                    side=residual_plan.get("side"),
                )
                return None
            if not bool(residual_plan.get("submit_expected")):
                _block_live_submit_plan(
                    reason="residual_submit_not_expected",
                    field_name="residual_submit_plan",
                    source=residual_plan.get("source"),
                    side=residual_plan.get("side"),
                )
                return None
            if _residual_live_sell_mode() != "enabled":
                _block_live_submit_plan(
                    reason="residual_live_sell_mode_not_enabled",
                    field_name="residual_submit_plan",
                    source=residual_plan.get("source"),
                    side=residual_plan.get("side"),
                )
                return None
            if bool(settings.LIVE_DRY_RUN) or not bool(settings.LIVE_REAL_ORDER_ARMED):
                _block_live_submit_plan(
                    reason="residual_live_real_order_not_armed",
                    field_name="residual_submit_plan",
                    source=residual_plan.get("source"),
                    side=residual_plan.get("side"),
                )
                return None
            try:
                if not _live_submit_plan_schema_valid(
                    residual_plan,
                    field_name="residual_submit_plan",
                ):
                    return None
                if not _validate_submit_authority_before_executor(
                    residual_plan,
                    plan_kind="residual",
                    field_name="residual_submit_plan",
                ):
                    return None
                return self.executor(
                    self.broker,
                    request.signal,
                    request.ts,
                    request.market_price,
                    strategy_name=request.strategy_name,
                    decision_id=request.decision_id,
                    decision_reason=request.decision_reason,
                    exit_rule_name=request.exit_rule_name,
                    execution_submit_plan=residual_plan,
                )
            except TypeError as exc:
                if "unexpected keyword argument" not in str(exc):
                    raise
                return {
                    "status": "blocked",
                    "reason": "executor_missing_execution_submit_plan_support",
                    "side": "SELL",
                    "source": "residual_inventory",
                    "authority": "residual_inventory_policy",
                }
        if target_plan or residual_plan or buy_plan:
            _log_live_submit_plan_block(
                reason="explicit_submit_plan_not_consumed",
                field_name="execution_decision",
                source=(target_plan or residual_plan or buy_plan).get("source"),
                side=(target_plan or residual_plan or buy_plan).get("side"),
            )
            return None
        if submit_plan_required:
            _log_live_submit_plan_block(
                reason="live_real_order_missing_execution_submit_plan",
                field_name="execution_decision",
                source=execution_decision.get("source"),
                side=request.signal,
            )
            return None
        harmless_dust_preview = None
        if request.signal == "SELL":
            harmless_dust_preview = _canonical_harmless_dust_sell_preview(
                getattr(request, "decision_context", None)
            )
        if harmless_dust_preview is not None:
            if self.record_harmless_dust_suppression_if_applicable(request):
                return None
        if str(settings.MODE).lower() == "live" and bool(settings.LIVE_DRY_RUN):
            _log_live_submit_plan_block(
                reason="live_dry_run_non_submitting",
                field_name="execution_service",
                source="legacy_lot_native_fallback",
                side=request.signal,
            )
            return None
        # Legacy lot-native compatibility path. Live real-order execution is
        # blocked above unless a validated explicit submit plan was consumed.
        try:
            return self.executor(
                self.broker,
                request.signal,
                request.ts,
                request.market_price,
                strategy_name=request.strategy_name,
                decision_id=request.decision_id,
                decision_reason=request.decision_reason,
                exit_rule_name=request.exit_rule_name,
            )
        except TypeError as exc:
            if "unexpected keyword argument" not in str(exc):
                raise
            return self.executor(self.broker, request.signal, request.ts, request.market_price)


def build_signal_execution_service(
    *,
    mode: str,
    broker: "Broker | None" = None,
    paper_executor: Callable[..., dict | None] = paper_execute,
    live_executor: Callable[..., dict | None] = live_execute_signal,
    harmless_dust_recorder: Callable[..., bool] = record_harmless_dust_exit_suppression,
) -> SignalExecutionService | None:
    if mode == "paper":
        return PaperSignalExecutionService(executor=paper_executor)
    if mode == "live" and broker is not None:
        return LiveSignalExecutionService(
            broker=broker,
            executor=live_executor,
            harmless_dust_recorder=harmless_dust_recorder,
        )
    return None
