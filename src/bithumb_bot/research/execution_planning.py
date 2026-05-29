from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bithumb_bot.execution_service import (
    ExecutionDecisionSummary,
    ExecutionReadinessPlanningInput,
    SignalExecutionRequest,
    ExecutionSubmitPlan,
    ExecutionTargetPlanningInput,
    TypedExecutionPlanningInput,
    build_typed_execution_decision_summary,
)
from bithumb_bot.lot_model import quantize_to_lot_count
from bithumb_bot.strategy_policy_contract import PositionSnapshot, StrategyDecisionV2


@dataclass(frozen=True)
class ResearchExecutionPlanBundle:
    submit_plan: ExecutionSubmitPlan | None
    source: str
    authority: str
    execution_engine: str
    status: str
    reason_code: str
    summary: ExecutionDecisionSummary | None = None
    compatibility_fallback: bool = False
    promotion_grade: bool = True
    recommended_next_action: str = "none"

    @property
    def submit_expected(self) -> bool:
        return bool(self.submit_plan is not None and self.submit_plan.submit_expected)


def _research_execution_submit_plan(
    *,
    side: str,
    cash: float,
    buy_fraction: float,
    sellable_qty: float,
    reference_price: float,
    policy_decision: StrategyDecisionV2 | None,
) -> ExecutionSubmitPlan:
    """Compatibility-only adapter for exploratory research strategies without typed plans."""
    normalized_side = str(side or "").upper()
    execution_intent = policy_decision.execution_intent if policy_decision is not None else None
    intent_payload = (
        execution_intent.as_dict()
        if execution_intent is not None and hasattr(execution_intent, "as_dict")
        else {}
    )
    authority = "strategy_execution_intent" if intent_payload else "research_compatibility_execution_intent"
    if normalized_side == "BUY":
        fraction = float(intent_payload.get("budget_fraction_of_cash") or buy_fraction)
        requested_notional = max(0.0, float(cash) * fraction)
        max_budget = float(intent_payload.get("max_budget_krw") or 0.0)
        if max_budget > 0.0:
            requested_notional = min(requested_notional, max_budget)
        qty = requested_notional / float(reference_price) if reference_price > 0.0 else None
        submit_expected = bool(requested_notional > 0.0)
        return ExecutionSubmitPlan(
            side="BUY",
            source="research_backtest",
            authority=authority,
            final_action="ENTER_STRATEGY_POSITION" if submit_expected else "BLOCK_RESEARCH_ZERO_SIZE",
            qty=qty,
            notional_krw=requested_notional if submit_expected else None,
            target_exposure_krw=requested_notional if submit_expected else None,
            current_effective_exposure_krw=0.0,
            delta_krw=requested_notional if submit_expected else None,
            submit_expected=submit_expected,
            pre_submit_proof_status="not_required",
            block_reason="none" if submit_expected else "research_zero_buy_notional",
            idempotency_key=None,
            extra_payload={"execution_engine": "research_virtual"},
        )
    if normalized_side == "SELL":
        qty = max(0.0, float(sellable_qty))
        notional = qty * float(reference_price) if reference_price > 0.0 else None
        submit_expected = bool(qty > 0.0)
        return ExecutionSubmitPlan(
            side="SELL",
            source="research_backtest",
            authority=authority,
            final_action="EXIT_STRATEGY_POSITION" if submit_expected else "BLOCK_RESEARCH_ZERO_SIZE",
            qty=qty if submit_expected else None,
            notional_krw=notional if submit_expected else None,
            target_exposure_krw=0.0 if submit_expected else None,
            current_effective_exposure_krw=notional if submit_expected else None,
            delta_krw=-(notional or 0.0) if submit_expected else None,
            submit_expected=submit_expected,
            pre_submit_proof_status="not_required",
            block_reason="none" if submit_expected else "research_zero_sell_qty",
            idempotency_key=None,
            extra_payload={"execution_engine": "research_virtual"},
        )
    raise ValueError(f"research_submit_plan_unsupported_side:{normalized_side or 'missing'}")


def _research_execution_plan_bundle(
    *,
    side: str,
    cash: float,
    buy_fraction: float,
    sellable_qty: float,
    reference_price: float,
    policy_decision: StrategyDecisionV2 | None,
    candle_ts: int,
    allow_compatibility_fallback: bool = False,
    promotion_grade_required: bool = True,
    block_reason: str = "",
) -> ResearchExecutionPlanBundle:
    normalized_side = str(side or "HOLD").upper()
    if normalized_side not in {"BUY", "SELL"}:
        return ResearchExecutionPlanBundle(
            submit_plan=None,
            summary=None,
            source="research_backtest",
            authority="research_virtual_execution_planner",
            execution_engine="research_virtual",
            status="BLOCKED",
            reason_code=block_reason or "research_no_submit_signal",
        )
    if policy_decision is not None:
        summary = build_typed_execution_decision_summary(
            typed_input=TypedExecutionPlanningInput(
                strategy_decision=policy_decision,
                candle_ts=int(candle_ts),
                market_price=float(reference_price),
                readiness=ExecutionReadinessPlanningInput.from_payload(
                    {
                        "cash_available": float(cash),
                        "total_effective_exposure_notional_krw": (
                            max(0.0, float(sellable_qty) * float(reference_price))
                        ),
                        "residual_inventory_policy_allows_run": True,
                    }
                ),
                target=ExecutionTargetPlanningInput(previous_target_exposure_krw=0.0),
            )
        )
        submit_plan = (
            summary.typed_target_submit_plan()
            or summary.typed_residual_submit_plan()
            or summary.typed_buy_submit_plan()
        )
        if (
            submit_plan is None
            and str(policy_decision.final_signal or "").upper() == "SELL"
            and bool(summary.submit_expected)
            and str(summary.final_action) == "EXIT_STRATEGY_POSITION"
        ):
            submit_plan = _research_execution_submit_plan(
                side="SELL",
                cash=cash,
                buy_fraction=buy_fraction,
                sellable_qty=sellable_qty,
                reference_price=reference_price,
                policy_decision=policy_decision,
            )
        if (
            promotion_grade_required
            and normalized_side in {"BUY", "SELL"}
            and submit_plan is None
            and not allow_compatibility_fallback
        ):
            raise ValueError("research_submit_plan_missing")
        if promotion_grade_required and bool(summary.submit_expected) and submit_plan is None:
            raise ValueError(summary.block_reason or "research_typed_submit_plan_missing")
        return ResearchExecutionPlanBundle(
            submit_plan=submit_plan,
            summary=summary,
            source="typed_execution_planner" if submit_plan is None else submit_plan.source,
            authority="typed_execution_planner" if submit_plan is None else submit_plan.authority,
            execution_engine="research_virtual",
            status="PLANNED" if submit_plan is not None and submit_plan.submit_expected else "BLOCKED",
            reason_code=(
                "none"
                if submit_plan is not None and submit_plan.submit_expected
                else summary.block_reason or "research_typed_submit_plan_missing"
            ),
        )
    if not allow_compatibility_fallback:
        return ResearchExecutionPlanBundle(
            submit_plan=None,
            summary=None,
            source="research_backtest",
            authority="typed_execution_planner_required",
            execution_engine="research_virtual",
            status="BLOCKED",
            reason_code=block_reason or "research_compatibility_submit_plan_disabled",
        )
    if promotion_grade_required:
        return ResearchExecutionPlanBundle(
            submit_plan=None,
            summary=None,
            source="research_backtest",
            authority="typed_execution_planner_required",
            execution_engine="research_virtual",
            status="BLOCKED",
            reason_code=block_reason or "promotion_requires_typed_execution_submit_plan",
            promotion_grade=False,
            recommended_next_action="regenerate_research_decisions_with_typed_execution_submit_plan",
        )
    submit_plan = _research_execution_submit_plan(
        side=normalized_side,
        cash=cash,
        buy_fraction=buy_fraction,
        sellable_qty=sellable_qty,
        reference_price=reference_price,
        policy_decision=policy_decision,
    )
    return ResearchExecutionPlanBundle(
        submit_plan=submit_plan,
        summary=None,
        source=submit_plan.source,
        authority=submit_plan.authority,
        execution_engine="research_virtual",
        status="PLANNED" if submit_plan.submit_expected else "BLOCKED",
        reason_code="none" if submit_plan.submit_expected else submit_plan.block_reason,
        compatibility_fallback=True,
        promotion_grade=False,
        recommended_next_action="regenerate_research_decisions_with_typed_execution_submit_plan",
    )


def _execution_plan_evidence(plan_bundle: ResearchExecutionPlanBundle | None) -> dict[str, object]:
    from bithumb_bot.canonical_decision import canonical_payload_hash

    submit_plan = None if plan_bundle is None else plan_bundle.submit_plan
    if submit_plan is None:
        reason_code = "" if plan_bundle is None else plan_bundle.reason_code
        final_action = "HOLD" if reason_code in {"", "research_no_submit_signal"} else "BLOCK_RESEARCH_NO_SUBMIT"
        summary_payload = {
            "final_action": final_action,
            "submit_expected": False,
            "pre_submit_proof_status": "not_required",
            "block_reason": reason_code or "none",
            "primary_submit_plan": None,
            "execution_engine": "none",
        }
        return {
            "execution_summary_hash": canonical_payload_hash(summary_payload),
            "execution_submit_plan_hash": canonical_payload_hash(None),
            "final_action": final_action,
            "submit_expected": False,
            "pre_submit_proof_status": "not_required",
            "execution_block_reason": reason_code or "none",
            "submit_plan_source": "none",
            "submit_plan_authority": "none",
            "execution_engine": "none",
            "execution_scope": "submit_plan_admission_only",
            "scope_badge": "SUBMIT_PLAN_EQUIVALENCE_ONLY",
            "execution_plan_bundle_present": plan_bundle is not None,
            "execution_plan_status": "" if plan_bundle is None else plan_bundle.status,
            "execution_plan_reason_code": "" if plan_bundle is None else plan_bundle.reason_code,
            "typed_execution_service": False,
            "typed_submit_plan": False,
            "typed_execution_boundary": "none",
            "research_compatibility_execution_fallback": (
                False if plan_bundle is None else bool(plan_bundle.compatibility_fallback)
            ),
            "compatibility_fallback": False if plan_bundle is None else bool(plan_bundle.compatibility_fallback),
            "promotion_grade": (
                True
                if plan_bundle is None
                else bool(plan_bundle.promotion_grade and not plan_bundle.compatibility_fallback)
            ),
            "recommended_next_action": "none" if plan_bundle is None else plan_bundle.recommended_next_action,
        }
    plan_payload = submit_plan.as_dict()
    summary_payload_for_engine = None if plan_bundle.summary is None else plan_bundle.summary.as_dict()
    execution_engine = str(
        (summary_payload_for_engine or {}).get("execution_engine")
        or plan_bundle.execution_engine
        or "research_virtual"
    )
    summary_payload = {
        "final_action": submit_plan.final_action,
        "submit_expected": bool(submit_plan.submit_expected),
        "pre_submit_proof_status": submit_plan.pre_submit_proof_status,
        "block_reason": submit_plan.block_reason,
        "primary_submit_plan": plan_payload,
        "execution_engine": execution_engine,
    }
    return {
        "execution_summary_hash": canonical_payload_hash(summary_payload),
        "execution_submit_plan_hash": canonical_payload_hash(plan_payload),
        "final_action": submit_plan.final_action,
        "submit_expected": bool(submit_plan.submit_expected),
        "pre_submit_proof_status": submit_plan.pre_submit_proof_status,
        "execution_block_reason": submit_plan.block_reason,
        "submit_plan_source": submit_plan.source,
        "submit_plan_authority": submit_plan.authority,
        "execution_engine": execution_engine,
        "execution_scope": "submit_plan_admission_only",
        "scope_badge": "SUBMIT_PLAN_EQUIVALENCE_ONLY",
        "execution_plan_bundle_present": True,
        "execution_plan_status": "PLANNED" if submit_plan.submit_expected else "BLOCKED",
        "execution_plan_reason_code": "none" if submit_plan.submit_expected else submit_plan.block_reason,
        "typed_execution_service": True,
        "typed_submit_plan": isinstance(submit_plan, ExecutionSubmitPlan),
        "typed_execution_boundary": "SignalExecutionRequest",
        "research_compatibility_execution_fallback": bool(plan_bundle.compatibility_fallback),
        "compatibility_fallback": bool(plan_bundle.compatibility_fallback),
        "promotion_grade": bool(plan_bundle.promotion_grade and not plan_bundle.compatibility_fallback),
        "recommended_next_action": plan_bundle.recommended_next_action,
    }


def _research_position_snapshot(
    *,
    qty: float,
    sellable_qty: float,
    pending_buy_qty: float,
    pending_sell_qty: float,
    entry_ts: int | None,
    entry_price: float | None,
    candle_ts: int,
    market_price: float,
) -> PositionSnapshot:
    if pending_buy_qty > 1e-12 or pending_sell_qty > 1e-12:
        open_lots = _research_lot_count(qty)
        reserved_lots = open_lots if pending_sell_qty > 1e-12 and open_lots > 0 else 0
        return PositionSnapshot(
            in_position=bool(qty > 1e-12),
            entry_allowed=False,
            exit_allowed=False,
            entry_block_reason="research_pending_fill_not_policy_comparable",
            exit_block_reason="research_pending_fill_not_policy_comparable",
            terminal_state="research_pending_fill_not_policy_comparable",
            entry_ts=entry_ts,
            entry_price=entry_price,
            qty_open=float(qty),
            raw_qty_open=float(qty),
            raw_total_asset_qty=float(qty),
            open_lot_count=open_lots,
            reserved_exit_lot_count=reserved_lots,
            sellable_executable_lot_count=0,
            dust_classification="no_dust",
            dust_state="no_dust",
            effective_flat=True,
            has_executable_exposure=bool(qty > 1e-12),
            has_any_position_residue=bool(qty > 1e-12),
        )
    if sellable_qty > 1e-12:
        holding_time_sec = max(0.0, (int(candle_ts) - int(entry_ts)) / 1000.0) if entry_ts is not None else 0.0
        unrealized_pnl = (
            (float(market_price) - float(entry_price)) * float(sellable_qty)
            if entry_price is not None
            else 0.0
        )
        unrealized_pnl_ratio = (
            ((float(market_price) - float(entry_price)) / float(entry_price))
            if entry_price not in (None, 0.0)
            else 0.0
        )
        return PositionSnapshot(
            in_position=True,
            entry_allowed=False,
            exit_allowed=True,
            entry_block_reason="position_has_executable_exposure",
            exit_block_reason="none",
            terminal_state="research_simulated_open_exposure",
            entry_ts=entry_ts,
            entry_price=entry_price,
            qty_open=float(sellable_qty),
            holding_time_sec=holding_time_sec,
            unrealized_pnl=unrealized_pnl,
            unrealized_pnl_ratio=unrealized_pnl_ratio,
            raw_qty_open=float(qty),
            raw_total_asset_qty=float(qty),
            open_lot_count=_research_lot_count(sellable_qty),
            sellable_executable_lot_count=_research_lot_count(sellable_qty),
            dust_classification="no_dust",
            dust_state="no_dust",
            effective_flat=False,
            has_executable_exposure=True,
            has_any_position_residue=True,
        )
    return PositionSnapshot(
        in_position=False,
        entry_allowed=True,
        exit_allowed=False,
        entry_block_reason="none",
        exit_block_reason="no_position",
        terminal_state="research_simulated_flat",
        dust_classification="no_dust",
        dust_state="no_dust",
    )


def execute_research_signal_request(
    *,
    service_cls: type[Any],
    execution_model: Any,
    fee_rate: float,
    signal: str,
    signal_ts: int,
    market_price: float,
    strategy_name: str,
    decision_reason: str,
    plan_bundle: ResearchExecutionPlanBundle,
    research_execution_context: Any,
) -> Any:
    service = service_cls(execution_model=execution_model, fee_rate=fee_rate)
    return service.execute(
        SignalExecutionRequest(
            signal=str(signal),
            ts=int(signal_ts),
            market_price=float(market_price),
            strategy_name=strategy_name,
            decision_reason=decision_reason,
            execution_decision_summary=plan_bundle.summary,
            execution_plan_bundle=plan_bundle,
            research_execution_context=research_execution_context,
        )
    )


def _research_lot_count(qty: float) -> int:
    return quantize_to_lot_count(qty=max(0.0, float(qty)), lot_size=0.0001)


__all__ = [
    "ResearchExecutionPlanBundle",
    "_execution_plan_evidence",
    "execute_research_signal_request",
    "_research_execution_plan_bundle",
    "_research_execution_submit_plan",
    "_research_position_snapshot",
]
