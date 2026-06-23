from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping

from ..broker.base import BrokerError
from ..decision_equivalence import sha256_prefixed
from ..execution_service import (
    ExecutionObservabilityPayload,
    SignalExecutionRequest,
    TypedExecutionRequest,
    execution_submit_plan_invariant_error,
    primary_execution_submit_plan,
)
from .lifecycle_artifacts import StateTransitionResult
from ..order_settlement import OrderSettlementResult


@dataclass(frozen=True)
class ExecutionCycleResult:
    candle_ts: int
    decision_id: int | None
    planning_status: str
    submit_expected: bool
    submitted: bool
    post_trade_reconciled: bool
    mark_processed_allowed: bool
    halt_transition: Mapping[str, Any] | None = None
    trade: Mapping[str, Any] | None = None
    settlement_result: Mapping[str, Any] | None = None
    notification_event_hashes: tuple[str, ...] = ()
    input_hash: str | None = None
    evidence_hash: str | None = None
    decision_hash: str | None = None
    pre_submit_risk_decision_hash: str | None = None
    pre_submit_risk_policy_hash: str | None = None
    pre_submit_risk_input_hash: str | None = None
    pre_submit_risk_evidence_hash: str | None = None
    pre_submit_risk_plan_hash: str | None = None
    pre_submit_risk_state_source: str | None = None
    pre_submit_risk_status: str | None = None
    pre_submit_risk_reason_code: str | None = None

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "artifact_type": "execution_cycle_result",
            "schema_version": 1,
            "candle_ts": self.candle_ts,
            "decision_id": self.decision_id,
            "planning_status": self.planning_status,
            "submit_expected": bool(self.submit_expected),
            "submitted": bool(self.submitted),
            "post_trade_reconciled": bool(self.post_trade_reconciled),
            "mark_processed_allowed": bool(self.mark_processed_allowed),
            "halt_transition": dict(self.halt_transition or {}),
            "trade_present": self.trade is not None,
            "settlement_result": dict(self.settlement_result or {}),
            "notification_event_hashes": list(self.notification_event_hashes),
            "input_hash": self.input_hash
            or sha256_prefixed({"candle_ts": self.candle_ts, "decision_id": self.decision_id}),
            "evidence_hash": self.evidence_hash
            or sha256_prefixed(
                {
                    "planning_status": self.planning_status,
                    "submit_expected": bool(self.submit_expected),
                    "submitted": bool(self.submitted),
                    "post_trade_reconciled": bool(self.post_trade_reconciled),
                    "settled": bool((self.settlement_result or {}).get("settled")),
                    "pre_submit_risk_status": self.pre_submit_risk_status,
                    "pre_submit_risk_reason_code": self.pre_submit_risk_reason_code,
                    "pre_submit_risk_decision_hash": self.pre_submit_risk_decision_hash,
                }
            ),
            "pre_submit_risk_decision_hash": self.pre_submit_risk_decision_hash,
            "pre_submit_risk_policy_hash": self.pre_submit_risk_policy_hash,
            "pre_submit_risk_input_hash": self.pre_submit_risk_input_hash,
            "pre_submit_risk_evidence_hash": self.pre_submit_risk_evidence_hash,
            "pre_submit_risk_plan_hash": self.pre_submit_risk_plan_hash,
            "pre_submit_risk_state_source": self.pre_submit_risk_state_source,
            "pre_submit_risk_status": self.pre_submit_risk_status,
            "pre_submit_risk_reason_code": self.pre_submit_risk_reason_code,
        }
        payload["decision_hash"] = self.decision_hash or sha256_prefixed(payload)
        return payload


@dataclass(frozen=True)
class ExecutionCoordinator:
    execution_engine_name: str

    def resolve_submit_expectation(self, summary: Any) -> TypedExecutionSubmitExpectation:
        return resolve_typed_execution_submit_expectation(
            summary,
            execution_engine_name=self.execution_engine_name,
        )

    def target_delta_submit_expected(self, *, submit_expected: bool) -> bool:
        return self.execution_engine_name.strip().lower() == "target_delta" and bool(submit_expected)

    def execute_cycle(
        self,
        *,
        candle_ts: int,
        decision_id: int | None,
        signal: str | None = None,
        market_price: float | None = None,
        strategy_name: str | None = None,
        decision_reason: str | None = None,
        exit_rule_name: str | None = None,
        decision_context: dict[str, object] | None = None,
        execution_plan_bundle: object | None = None,
        execution_decision_summary: Any,
        execution_service: Any | None = None,
        submit_invoker: Callable[[], Any] | None = None,
        post_trade_reconcile: Callable[[], Any] | None = None,
        settlement_coordinator: Callable[[Mapping[str, Any]], OrderSettlementResult | Mapping[str, Any]]
        | None = None,
        settlement_required: bool = False,
        input_hash: str | None = None,
        execution_plan_bundle_hash: str | None = None,
    ) -> ExecutionCycleResult:
        if decision_id is None:
            return ExecutionCycleResult(
                candle_ts=candle_ts,
                decision_id=None,
                planning_status="decision_persistence_failed",
                submit_expected=False,
                submitted=False,
                post_trade_reconciled=False,
                mark_processed_allowed=False,
                input_hash=input_hash,
            )
        if execution_decision_summary is None:
            return ExecutionCycleResult(
                candle_ts=candle_ts,
                decision_id=decision_id,
                planning_status="execution_summary_missing",
                submit_expected=False,
                submitted=False,
                post_trade_reconciled=False,
                mark_processed_allowed=False,
                input_hash=input_hash,
            )
        expectation = self.resolve_submit_expectation(execution_decision_summary)
        bundle_submit_plan = getattr(execution_plan_bundle, "submit_plan", None)
        primary_plan = (
            bundle_submit_plan
            if callable(getattr(bundle_submit_plan, "content_hash", None))
            else primary_execution_submit_plan(execution_decision_summary)
        )
        batch_error = _batch_selected_pair_plan_error(
            execution_plan_bundle=execution_plan_bundle,
            primary_plan=primary_plan,
            decision_context=decision_context,
            require_batch=execution_plan_bundle is not None or execution_service is not None,
        )
        if batch_error is not None:
            return ExecutionCycleResult(
                candle_ts=candle_ts,
                decision_id=decision_id,
                planning_status=batch_error,
                submit_expected=False,
                submitted=False,
                post_trade_reconciled=False,
                mark_processed_allowed=True,
                input_hash=input_hash,
            )
        invariant_error = execution_submit_plan_invariant_error(
            primary_plan,
            compatibility_signal=signal or "HOLD",
        )
        if invariant_error is not None:
            return ExecutionCycleResult(
                candle_ts=candle_ts,
                decision_id=decision_id,
                planning_status=invariant_error,
                submit_expected=False,
                submitted=False,
                post_trade_reconciled=False,
                mark_processed_allowed=True,
                input_hash=input_hash,
            )
        if not expectation.submit_expected:
            if execution_service is not None and signal == "SELL":
                request = build_signal_execution_request(
                    signal=authoritative_execution_signal_for_trade(
                        decision_context,
                        fallback_signal=signal or "HOLD",
                    ),
                    ts=candle_ts,
                    market_price=float(market_price or 0.0),
                    strategy_name=strategy_name,
                    decision_id=decision_id,
                    decision_reason=decision_reason,
                    exit_rule_name=exit_rule_name,
                    execution_decision_summary=execution_decision_summary,
                    decision_context=decision_context,
                    execution_plan_bundle=execution_plan_bundle,
                )
                suppressor = getattr(
                    execution_service,
                    "record_harmless_dust_suppression_if_applicable",
                    None,
                )
                if callable(suppressor):
                    suppressor(request)
            return ExecutionCycleResult(
                candle_ts=candle_ts,
                decision_id=decision_id,
                planning_status="submit_blocked",
                submit_expected=bool(expectation.submit_expected),
                submitted=False,
                post_trade_reconciled=False,
                mark_processed_allowed=True,
                input_hash=input_hash,
            )
        if execution_service is not None:
            submit_invoker = lambda: execution_service.execute(
                build_signal_execution_request(
                    signal=authoritative_execution_signal_for_trade(
                        decision_context,
                        fallback_signal=signal or "HOLD",
                    ),
                    ts=candle_ts,
                    market_price=float(market_price or 0.0),
                    strategy_name=strategy_name,
                    decision_id=decision_id,
                    decision_reason=decision_reason,
                    exit_rule_name=exit_rule_name,
                    execution_decision_summary=execution_decision_summary,
                    decision_context=decision_context,
                    execution_plan_bundle=execution_plan_bundle,
                )
            )
        if submit_invoker is None:
            return ExecutionCycleResult(
                candle_ts=candle_ts,
                decision_id=decision_id,
                planning_status="submit_boundary_missing",
                submit_expected=True,
                submitted=False,
                post_trade_reconciled=False,
                mark_processed_allowed=True,
                input_hash=input_hash,
            )
        try:
            trade = submit_invoker()
        except BrokerError as exc:
            return self._halted_result(
                candle_ts=candle_ts,
                decision_id=decision_id,
                planning_status="live_execution_broker_error",
                reason_code="LIVE_EXECUTION_BROKER_ERROR",
                error=f"live execution broker error ({type(exc).__name__}): {exc}",
                input_hash=input_hash,
                execution_plan_bundle_hash=execution_plan_bundle_hash,
            )
        except Exception as exc:
            return self._halted_result(
                candle_ts=candle_ts,
                decision_id=decision_id,
                planning_status="live_execution_failed",
                reason_code="LIVE_EXECUTION_FAILED",
                error=f"live execution failed ({type(exc).__name__}): {exc}",
                input_hash=input_hash,
                execution_plan_bundle_hash=execution_plan_bundle_hash,
            )
        pre_submit_fields = _execution_time_pre_submit_fields(execution_service)
        if trade is None:
            return ExecutionCycleResult(
                candle_ts=candle_ts,
                decision_id=decision_id,
                planning_status="submit_blocked",
                submit_expected=True,
                submitted=False,
                post_trade_reconciled=False,
                mark_processed_allowed=True,
                input_hash=input_hash,
                **pre_submit_fields,
            )
        try:
            if post_trade_reconcile is not None:
                post_trade_reconcile()
        except Exception as exc:
            return self._halted_result(
                candle_ts=candle_ts,
                decision_id=decision_id,
                planning_status="post_trade_reconcile_failed",
                reason_code="POST_TRADE_RECONCILE_FAILED",
                error=f"reconcile failed ({type(exc).__name__}): {exc}",
                input_hash=input_hash,
                execution_plan_bundle_hash=execution_plan_bundle_hash,
                submitted=True,
            )
        settlement_payload: Mapping[str, Any] | None = None
        if settlement_coordinator is not None and isinstance(trade, Mapping):
            try:
                settlement = settlement_coordinator(trade)
            except Exception as exc:
                return self._halted_result(
                    candle_ts=candle_ts,
                    decision_id=decision_id,
                    planning_status="order_settlement_failed",
                    reason_code="ORDER_SETTLEMENT_FAILED",
                    error=f"settlement failed ({type(exc).__name__}): {exc}",
                    input_hash=input_hash,
                    execution_plan_bundle_hash=execution_plan_bundle_hash,
                    submitted=True,
                )
            settlement_payload = (
                settlement.as_dict()
                if callable(getattr(settlement, "as_dict", None))
                else dict(settlement)
            )
        mark_processed_allowed = True
        if settlement_payload is not None:
            mark_processed_allowed = bool(settlement_payload.get("settled"))
        elif settlement_required:
            mark_processed_allowed = False
        return ExecutionCycleResult(
            candle_ts=candle_ts,
            decision_id=decision_id,
            planning_status="submitted",
            submit_expected=True,
            submitted=True,
            post_trade_reconciled=post_trade_reconcile is not None,
            mark_processed_allowed=mark_processed_allowed,
            input_hash=input_hash,
            trade=trade if isinstance(trade, Mapping) else None,
            settlement_result=settlement_payload,
            **pre_submit_fields,
        )

    def _halted_result(
        self,
        *,
        candle_ts: int,
        decision_id: int | None,
        planning_status: str,
        reason_code: str,
        error: str,
        input_hash: str | None,
        execution_plan_bundle_hash: str | None,
        submitted: bool = False,
    ) -> ExecutionCycleResult:
        transition = StateTransitionResult(
            status="pending",
            reason_code=reason_code,
            state_from="READY",
            state_to="HALTED",
            applied=False,
            evidence={"error": error},
        )
        return ExecutionCycleResult(
            candle_ts=candle_ts,
            decision_id=decision_id,
            planning_status=planning_status,
            submit_expected=True,
            submitted=submitted,
            post_trade_reconciled=False,
            mark_processed_allowed=True,
            halt_transition=transition.as_dict(),
            input_hash=input_hash,
        )


def _execution_time_pre_submit_fields(execution_service: Any | None) -> dict[str, str | None]:
    payload = getattr(execution_service, "last_pre_submit_risk_payload", None)
    if not isinstance(payload, Mapping):
        return {}
    fields = {
        key: payload.get(key)
        for key in (
            "pre_submit_risk_decision_hash",
            "pre_submit_risk_policy_hash",
            "pre_submit_risk_input_hash",
            "pre_submit_risk_evidence_hash",
            "pre_submit_risk_plan_hash",
            "pre_submit_risk_state_source",
            "pre_submit_risk_status",
            "pre_submit_risk_reason_code",
        )
    }
    return {
        key: (None if value is None else str(value))
        for key, value in fields.items()
    }


__all__ = [
    "ExecutionCoordinator",
    "ExecutionCycleResult",
    "TypedExecutionSubmitExpectation",
    "resolve_typed_execution_submit_expectation",
    "authoritative_execution_signal_for_trade",
    "build_signal_execution_request",
]


def build_signal_execution_request(
    *,
    signal: str,
    ts: int,
    market_price: float,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
    execution_decision_summary: object | None,
    decision_context: dict[str, object] | None,
    execution_plan_bundle: object | None = None,
) -> SignalExecutionRequest:
    typed_request = TypedExecutionRequest(
        signal=signal,
        ts=ts,
        market_price=market_price,
        strategy_name=strategy_name,
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
        execution_decision_summary=execution_decision_summary,
        execution_plan_bundle=execution_plan_bundle,
    )
    request = SignalExecutionRequest.from_typed(
        typed_request,
        observability_payload=ExecutionObservabilityPayload(decision_context or {}),
    )
    return SignalExecutionRequest(
        signal=request.signal,
        ts=request.ts,
        market_price=request.market_price,
        strategy_name=request.strategy_name,
        decision_id=request.decision_id,
        decision_reason=request.decision_reason,
        exit_rule_name=request.exit_rule_name,
        execution_decision_summary=request.execution_decision_summary,
        execution_plan_bundle=request.execution_plan_bundle,
        observability_payload=request.observability_payload,
        research_execution_context=request.research_execution_context,
        decision_context=decision_context,
        observability_context=decision_context,
    )


def authoritative_execution_signal_for_trade(
    decision_context: dict[str, object] | None,
    *,
    fallback_signal: object,
) -> str:
    if isinstance(decision_context, dict):
        planned = str(decision_context.get("authoritative_execution_signal") or "").strip().upper()
        if planned in {"BUY", "SELL", "HOLD"}:
            return planned
        execution_decision = decision_context.get("execution_decision")
        if isinstance(execution_decision, dict):
            planned = str(execution_decision.get("final_signal") or "").strip().upper()
            if planned in {"BUY", "SELL", "HOLD"}:
                return planned
    fallback = str(fallback_signal or "HOLD").strip().upper()
    return fallback if fallback in {"BUY", "SELL", "HOLD"} else "HOLD"


def _batch_selected_pair_plan_error(
    *,
    execution_plan_bundle: object | None,
    primary_plan: object | None,
    decision_context: Mapping[str, object] | None,
    require_batch: bool,
) -> str | None:
    batch = getattr(execution_plan_bundle, "execution_plan_batch", None)
    if batch is None:
        return "execution_plan_batch_missing" if require_batch else None
    pair_plans = tuple(getattr(batch, "pair_plans", ()) or ())
    if not pair_plans:
        return "execution_plan_batch_pair_plans_missing"
    if len(pair_plans) != 1:
        return "execution_plan_batch_single_pair_required"
    pair_plan = pair_plans[0]
    runtime_pair = ""
    if isinstance(decision_context, Mapping):
        runtime_pair = str(decision_context.get("runtime_pair") or "").strip()
    if not runtime_pair and primary_plan is not None:
        runtime_pair = str(getattr(primary_plan, "pair", "") or "").strip()
    pair_plan_pair = str(getattr(pair_plan, "pair", "") or "").strip()
    if not pair_plan_pair:
        return "execution_plan_batch_pair_plan_pair_missing"
    if runtime_pair and pair_plan_pair != runtime_pair:
        return "execution_plan_batch_pair_mismatch"
    if len({str(getattr(plan, "pair", "") or "").strip() for plan in pair_plans}) != len(pair_plans):
        return "execution_plan_batch_duplicate_pair_plan"
    if primary_plan is not None:
        submit_hash = primary_plan.content_hash() if callable(getattr(primary_plan, "content_hash", None)) else ""
        if str(getattr(pair_plan, "execution_submit_plan_hash", "") or "") != submit_hash:
            return "execution_plan_batch_submit_plan_hash_mismatch"
        if bool(getattr(primary_plan, "submit_expected", False)) and not tuple(
            getattr(pair_plan, "scope_key_hashes", ()) or ()
        ):
            return "execution_plan_batch_scope_evidence_missing"
        if bool(getattr(primary_plan, "submit_expected", False)) and not str(
            getattr(pair_plan, "order_rule_snapshot_hash", "") or ""
        ).strip():
            return "execution_plan_batch_order_rule_evidence_missing"
        if bool(getattr(primary_plan, "submit_expected", False)):
            pre_submit_required = bool(getattr(pair_plan, "pre_submit_risk_required", False))
            pre_submit_hash = str(getattr(pair_plan, "pre_submit_risk_decision_hash", "") or "").strip()
            finalization_required = bool(
                getattr(pair_plan, "pre_submit_risk_finalization_required", False)
            )
            not_required_reason = str(
                getattr(pair_plan, "pre_submit_risk_not_required_reason", "") or ""
            ).strip()
            if pre_submit_required and not pre_submit_hash and not finalization_required:
                return "execution_plan_batch_pre_submit_risk_proof_missing"
            if not pre_submit_required and not not_required_reason:
                return "execution_plan_batch_pre_submit_risk_not_required_reason_missing"
        if bool(getattr(primary_plan, "submit_expected", False)):
            if not str(getattr(pair_plan, "lock_evidence_hash", "") or "").strip():
                return "execution_plan_batch_lock_evidence_missing"
            lock_status = str(getattr(pair_plan, "lock_status", "") or "").strip()
            if lock_status == "intent_pending_persistence" and not _lock_intent_persisted_for_pair_plan(
                pair_plan=pair_plan,
                decision_context=decision_context,
            ):
                return "execution_plan_batch_lock_persistence_missing"
            if lock_status not in {"active", "not_required", "intent_pending_persistence"}:
                return "execution_plan_batch_lock_status_invalid"
    batch_hash = batch.content_hash() if callable(getattr(batch, "content_hash", None)) else ""
    if isinstance(decision_context, Mapping):
        expected_batch_hash = str(decision_context.get("execution_plan_batch_hash") or "").strip()
        if expected_batch_hash and expected_batch_hash != batch_hash:
            return "execution_plan_batch_hash_mismatch"
        expected_pair_hash = str(decision_context.get("pair_execution_plan_hash") or "").strip()
        pair_hash = pair_plan.content_hash() if callable(getattr(pair_plan, "content_hash", None)) else ""
        if expected_pair_hash and expected_pair_hash != pair_hash:
            return "pair_execution_plan_hash_mismatch"
    return None


def _lock_intent_persisted_for_pair_plan(
    *,
    pair_plan: object,
    decision_context: Mapping[str, object] | None,
) -> bool:
    if not isinstance(decision_context, Mapping):
        return False
    if decision_context.get("decision_id") is None or decision_context.get("execution_plan_id") is None:
        return False
    if decision_context.get("decision_persistence_transaction_elapsed_ms") is None:
        return False
    pair_lock_hash = str(getattr(pair_plan, "lock_evidence_hash", "") or "").strip()
    if not pair_lock_hash:
        return False
    for intent in decision_context.get("lock_intents") or ():
        if not isinstance(intent, Mapping):
            continue
        if str(intent.get("evidence_hash") or intent.get("lock_hash") or "").strip() == pair_lock_hash:
            return True
    return False


@dataclass(frozen=True)
class TypedExecutionSubmitExpectation:
    submit_expected: bool
    plan_source: str | None = None
    block_reason: str | None = None


def resolve_typed_execution_submit_expectation(
    summary: Any,
    *,
    execution_engine_name: str,
) -> TypedExecutionSubmitExpectation:
    if summary is None:
        return TypedExecutionSubmitExpectation(submit_expected=False)
    engine_name = str(execution_engine_name or "lot_native").strip().lower()
    if engine_name != "target_delta":
        return TypedExecutionSubmitExpectation(submit_expected=bool(summary.submit_expected))
    target_plan = summary.typed_target_submit_plan()
    if target_plan is None:
        return TypedExecutionSubmitExpectation(
            submit_expected=False,
            block_reason="missing_typed_target_submit_plan",
        )
    return TypedExecutionSubmitExpectation(
        submit_expected=bool(target_plan.submit_expected)
        and str(target_plan.block_reason or "none") == "none",
        plan_source=target_plan.source,
        block_reason=target_plan.block_reason,
    )
