from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from bithumb_bot.market_regime import aggregate_regime_coverage, aggregate_regime_performance
from bithumb_bot.lot_model import quantize_to_lot_count
from bithumb_bot.execution_service import (
    ExecutionReadinessPlanningInput,
    ExecutionDecisionSummary,
    ExecutionSubmitPlan,
    SignalExecutionRequest,
    ExecutionTargetPlanningInput,
    TypedExecutionPlanningInput,
    build_typed_execution_decision_summary,
    validate_execution_submit_plan_payload,
)
from bithumb_bot.strategy.exit_rules import merge_exit_rules
from bithumb_bot.strategy_policy_contract import PositionSnapshot, StrategyDecisionV2

from . import backtest_support as support
from .decision_event import ResearchDecisionEvent
from .execution_model import ExecutionFill, ExecutionModel, ExecutionRequest, FixedBpsExecutionModel
from .execution_timing import build_signal_event, candle_close_ts, resolve_execution_reference
from .experiment_manifest import ExecutionTimingPolicy, legacy_research_portfolio_policy
from .metrics_contract import EquityPoint, build_metrics_v2
from .strategy_spec import exit_policy_from_parameters, exit_policy_hash, strategy_spec_for_name

if TYPE_CHECKING:
    from .backtest_support import BacktestRun, BacktestRunContext
    from .dataset_snapshot import DatasetSnapshot
    from .execution_model import ExecutionModel
    from .experiment_manifest import ExecutionTimingPolicy, PortfolioPolicy


BacktestRun = support.BacktestRun
BacktestRunContext = support.BacktestRunContext
empty_execution_event_summary = support.empty_execution_event_summary
execution_event_summary = support.execution_event_summary


@dataclass(frozen=True)
class ResearchExecutionContext:
    signal_ts: int
    decision_ts: int
    timing_fields: dict[str, object]
    depth_fields: dict[str, object]

    def execution_request_fields(self) -> dict[str, object]:
        fields = dict(self.timing_fields)
        fields.update(dict(self.depth_fields))
        return fields


def execution_submit_plan_to_research_request(
    *,
    submit_plan: ExecutionSubmitPlan,
    context: ResearchExecutionContext,
    reference_price: float,
    fee_rate: float,
) -> ExecutionRequest | None:
    if not isinstance(submit_plan, ExecutionSubmitPlan):
        raise ValueError("research_submit_plan_not_typed")
    if not isinstance(context, ResearchExecutionContext):
        raise ValueError("research_execution_context_not_typed")
    payload = submit_plan.as_dict()
    validate_execution_submit_plan_payload(payload, field_name="research_submit_plan")
    if not bool(submit_plan.submit_expected):
        return None
    if str(submit_plan.block_reason or "none") not in {
        "none",
        "residual_buy_sizing_mode_telemetry",
    }:
        return None
    side = str(submit_plan.side or "").upper()
    if side == "BUY":
        requested_notional = _positive_float_or_none(submit_plan.notional_krw)
        if requested_notional is None:
            raise ValueError("research_buy_submit_plan_missing_size")
        if submit_plan.qty is None:
            if reference_price <= 0.0:
                raise ValueError("research_buy_submit_plan_missing_size")
            requested_qty = requested_notional / float(reference_price)
        else:
            requested_qty = _positive_float_or_none(submit_plan.qty)
            if requested_qty is None:
                raise ValueError("research_buy_submit_plan_missing_size")
    elif side == "SELL":
        requested_qty = _positive_float_or_none(submit_plan.qty)
        requested_notional = _positive_float_or_none(submit_plan.notional_krw)
        if requested_qty is None:
            raise ValueError("research_sell_submit_plan_missing_qty")
        if requested_notional is None:
            raise ValueError("research_sell_submit_plan_missing_notional")
    else:
        raise ValueError(f"research_submit_plan_unsupported_side:{side or 'missing'}")
    return ExecutionRequest(
        signal_ts=int(context.signal_ts),
        decision_ts=int(context.decision_ts),
        side=side,
        reference_price=float(reference_price),
        requested_qty=requested_qty,
        requested_notional=requested_notional,
        fee_rate=float(fee_rate),
        **context.execution_request_fields(),
    )


@dataclass(frozen=True)
class ResearchVirtualExecutionService:
    """Research execution adapter whose public boundary is SignalExecutionRequest."""

    execution_model: ExecutionModel
    fee_rate: float

    def execute(
        self,
        request: SignalExecutionRequest,
    ) -> ExecutionFill | None:
        if not isinstance(request, SignalExecutionRequest):
            raise ValueError("research_signal_execution_request_not_typed")
        context = request.research_execution_context
        if not isinstance(context, ResearchExecutionContext):
            raise ValueError("research_execution_context_not_typed")
        submit_plan = self._typed_submit_plan_from_request(request)
        if submit_plan is None:
            raise ValueError("research_missing_typed_submit_plan")
        return self.simulate_submit_plan(
            submit_plan=submit_plan,
            context=context,
            reference_price=float(request.market_price),
        )

    def _typed_submit_plan_from_request(
        self,
        request: SignalExecutionRequest,
    ) -> ExecutionSubmitPlan | None:
        bundle = request.execution_plan_bundle
        bundle_plan = getattr(bundle, "submit_plan", None) if bundle is not None else None
        if bundle is not None and bundle_plan is not None and not isinstance(bundle_plan, ExecutionSubmitPlan):
            raise ValueError("research_dict_only_submit_plan_not_authority")
        if isinstance(bundle_plan, ExecutionSubmitPlan):
            return bundle_plan
        summary = request.execution_decision_summary or getattr(bundle, "summary", None)
        if summary is None:
            return None
        if not isinstance(summary, ExecutionDecisionSummary):
            raise ValueError("research_execution_summary_not_typed")
        for field_name, candidate in (
            ("target_submit_plan", summary.target_submit_plan),
            ("residual_submit_plan", summary.residual_submit_plan),
            ("buy_submit_plan", summary.buy_submit_plan),
        ):
            if candidate is not None and not isinstance(candidate, ExecutionSubmitPlan):
                raise ValueError(f"research_dict_only_submit_plan_not_authority:{field_name}")
        return (
            summary.typed_target_submit_plan()
            or summary.typed_residual_submit_plan()
            or summary.typed_buy_submit_plan()
        )

    def simulate_submit_plan(
        self,
        *,
        submit_plan: ExecutionSubmitPlan,
        context: ResearchExecutionContext,
        reference_price: float,
    ) -> ExecutionFill | None:
        if not isinstance(submit_plan, ExecutionSubmitPlan):
            raise ValueError("research_submit_plan_not_typed")
        request = execution_submit_plan_to_research_request(
            submit_plan=submit_plan,
            context=context,
            reference_price=reference_price,
            fee_rate=float(self.fee_rate),
        )
        if request is None:
            return None
        return self.execution_model.simulate(request)


def _positive_float_or_none(value: object) -> float | None:
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0.0 else None


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
    """Compatibility-only adapter for legacy research strategies without typed plans."""
    normalized_side = str(side or "").upper()
    execution_intent = (
        policy_decision.execution_intent
        if policy_decision is not None
        else None
    )
    intent_payload = (
        execution_intent.as_dict()
        if execution_intent is not None and hasattr(execution_intent, "as_dict")
        else {}
    )
    authority = (
        "strategy_execution_intent"
        if intent_payload
        else "research_compatibility_execution_intent"
    )
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
        if promotion_grade_required and normalized_side in {"BUY", "SELL"} and submit_plan is None:
            raise ValueError("research_submit_plan_missing")
        if promotion_grade_required and bool(summary.submit_expected) and submit_plan is None:
            raise ValueError(summary.block_reason or "research_typed_submit_plan_missing")
        return ResearchExecutionPlanBundle(
            submit_plan=submit_plan,
            summary=summary,
            source="typed_execution_planner" if submit_plan is None else submit_plan.source,
            authority=(
                "typed_execution_planner"
                if submit_plan is None
                else submit_plan.authority
            ),
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


def _execution_plan_evidence(
    plan_bundle: ResearchExecutionPlanBundle | None,
) -> dict[str, object]:
    submit_plan = None if plan_bundle is None else plan_bundle.submit_plan
    if submit_plan is None:
        from bithumb_bot.canonical_decision import canonical_payload_hash

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
            "recommended_next_action": (
                "none" if plan_bundle is None else plan_bundle.recommended_next_action
            ),
        }
    from bithumb_bot.canonical_decision import canonical_payload_hash

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
        holding_time_sec = (
            max(0.0, (int(candle_ts) - int(entry_ts)) / 1000.0)
            if entry_ts is not None
            else 0.0
        )
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


def _research_lot_count(qty: float) -> int:
    return quantize_to_lot_count(qty=max(0.0, float(qty)), lot_size=0.0001)


@dataclass(frozen=True)
class BacktestKernel:
    """Stable common-kernel API for decision-event backtests."""

    def run(
        self,
        *,
        dataset: DatasetSnapshot,
        strategy_name: str,
        parameter_values: dict[str, Any],
        fee_rate: float,
        slippage_bps: float,
        decision_events: tuple[ResearchDecisionEvent, ...],
        parameter_stability_score: float | None = None,
        execution_model: ExecutionModel | None = None,
        execution_timing_policy: ExecutionTimingPolicy | None = None,
        portfolio_policy: PortfolioPolicy | None = None,
        context: BacktestRunContext | None = None,
    ) -> BacktestRun:
        return run_decision_event_backtest(
            dataset=dataset,
            strategy_name=strategy_name,
            parameter_values=parameter_values,
            fee_rate=fee_rate,
            slippage_bps=slippage_bps,
            decision_events=decision_events,
            parameter_stability_score=parameter_stability_score,
            execution_model=execution_model,
            execution_timing_policy=execution_timing_policy,
            portfolio_policy=portfolio_policy,
            context=context,
        )


def run_decision_event_backtest(
    *,
    dataset: DatasetSnapshot,
    strategy_name: str,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    decision_events: tuple[ResearchDecisionEvent, ...],
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
    execution_timing_policy: ExecutionTimingPolicy | None = None,
    portfolio_policy: PortfolioPolicy | None = None,
    context: BacktestRunContext | None = None,
) -> BacktestRun:
    return _run_decision_event_backtest_impl(
        dataset=dataset,
        strategy_name=strategy_name,
        parameter_values=parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
        decision_events=decision_events,
        parameter_stability_score=parameter_stability_score,
        execution_model=execution_model,
        execution_timing_policy=execution_timing_policy,
        portfolio_policy=portfolio_policy,
        context=context,
    )


def _run_decision_event_backtest_impl(
    *,
    dataset: DatasetSnapshot,
    strategy_name: str,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    decision_events: tuple[ResearchDecisionEvent, ...],
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
    execution_timing_policy: ExecutionTimingPolicy | None = None,
    portfolio_policy: PortfolioPolicy | None = None,
    context: BacktestRunContext | None = None,
) -> BacktestRun:
    """Execute strategy decision events through the shared research backtest kernel."""
    from .strategy_registry import resolve_research_strategy_plugin

    strategy_plugin = resolve_research_strategy_plugin(strategy_name)
    strategy_spec = strategy_spec_for_name(strategy_name)
    active_exit_policy = exit_policy_from_parameters(strategy_name, parameter_values)
    active_exit_policy_hash = exit_policy_hash(active_exit_policy)
    candles = dataset.candles
    run_context = context or BacktestRunContext(report_detail="full")
    timing_policy = execution_timing_policy or ExecutionTimingPolicy()
    policy = portfolio_policy or legacy_research_portfolio_policy()
    model = execution_model or FixedBpsExecutionModel(fee_rate=fee_rate, slippage_bps=slippage_bps)
    starting_cash = float(policy.starting_cash_krw)
    cash = starting_cash
    qty = float(policy.initial_position_qty)
    buy_fraction = float(policy.position_sizing.buy_fraction)
    accumulator = support.BacktestAccumulator(
        context=run_context,
        total_candles=len(candles),
        diagnostics_namespace=strategy_plugin.diagnostics_namespace,
    )
    if not candles:
        audit_trace_index = support.complete_audit_trace(run_context, status="completed")
        return BacktestRun(
            metrics=support.empty_metrics(parameter_stability_score),
            metrics_v2=support.empty_metrics_v2(starting_cash=starting_cash, initial_position_qty=qty),
            trades=(),
            candle_count=0,
            warnings=("not_enough_candles",),
            execution_event_summary=empty_execution_event_summary(),
            resource_usage=accumulator.resource_usage(candles_processed=0),
            strategy_diagnostics=accumulator.strategy_diagnostics(trades=[]),
            retained_detail_summary=support.retained_detail_summary(accumulator, retained_regime_snapshot_count=0),
            audit_trace_index=audit_trace_index,
        )

    dataset_content_hash = dataset.content_hash()
    candle_index_by_ts = {int(candle.ts): index for index, candle in enumerate(candles)}
    trades: list[dict[str, object]] = []
    decisions: list[dict[str, object]] = []
    equity_curve: list[EquityPoint] = []
    pending_fills: list[support.PendingFill] = []
    warnings: list[str] = []
    closed_pnls: list[float] = []
    entry_cost_basis = 0.0
    entry_regime_snapshot: dict[str, object] | None = None
    entry_ts: int | None = None
    entry_price: float | None = None
    entry_decision_hash: str | None = None
    open_trade_path: list[dict[str, float | int]] = []
    entry_fee = 0.0
    entry_slippage = 0.0
    fee_total = 0.0
    slippage_total = 0.0
    peak = starting_cash
    max_drawdown = 0.0
    regime_snapshots: list[dict[str, object]] = []
    regime_coverage_accumulator = support.RegimeCoverageAccumulator()

    first = candles[0]
    first_ts = candle_close_ts(first, interval=dataset.interval)
    retain_initial_equity = accumulator.retain_equity_point()
    if retain_initial_equity:
        equity_curve.append(EquityPoint(ts=first_ts, equity=starting_cash, cash=cash, asset_qty=qty))
    accumulator.update_equity(retained=retain_initial_equity, ts=first_ts, asset_qty=qty)
    support.trace_equity_mark(run_context, ts=first_ts, equity=starting_cash, cash=cash, asset_qty=qty)

    for event_number, event in enumerate(decision_events, start=1):
        if event.strategy_name != strategy_plugin.name:
            raise ValueError(f"decision_event_strategy_mismatch:{event.strategy_name}")
        index = candle_index_by_ts.get(int(event.candle_ts))
        if index is None:
            raise ValueError(f"decision_event_candle_missing:{event.candle_ts}")
        candle = candles[index]
        mark_boundary_ts = candle_close_ts(candle, interval=dataset.interval)
        decision_boundary_ts = int(event.decision_ts)
        (
            cash,
            qty,
            entry_cost_basis,
            entry_regime_snapshot,
            entry_ts,
            entry_price,
            entry_decision_hash,
            open_trade_path,
            entry_fee,
            entry_slippage,
            fee_total,
            slippage_total,
        ) = support.apply_pending_fills(
            pending_fills=pending_fills,
            trades=trades,
            boundary_ts=mark_boundary_ts,
            cash=cash,
            qty=qty,
            entry_cost_basis=entry_cost_basis,
            entry_regime_snapshot=entry_regime_snapshot,
            entry_ts=entry_ts,
            entry_price=entry_price,
            entry_decision_hash=entry_decision_hash,
            open_trade_path=open_trade_path,
            entry_fee=entry_fee,
            entry_slippage=entry_slippage,
            fee_total=fee_total,
            slippage_total=slippage_total,
            closed_pnls=closed_pnls,
        )
        mark_cash = cash
        mark_qty = qty
        (
            cash,
            qty,
            entry_cost_basis,
            entry_regime_snapshot,
            entry_ts,
            entry_price,
            entry_decision_hash,
            open_trade_path,
            entry_fee,
            entry_slippage,
            fee_total,
            slippage_total,
        ) = support.apply_pending_fills(
            pending_fills=pending_fills,
            trades=trades,
            boundary_ts=decision_boundary_ts,
            cash=cash,
            qty=qty,
            entry_cost_basis=entry_cost_basis,
            entry_regime_snapshot=entry_regime_snapshot,
            entry_ts=entry_ts,
            entry_price=entry_price,
            entry_decision_hash=entry_decision_hash,
            open_trade_path=open_trade_path,
            entry_fee=entry_fee,
            entry_slippage=entry_slippage,
            fee_total=fee_total,
            slippage_total=slippage_total,
            closed_pnls=closed_pnls,
        )
        if qty > 1e-12 and entry_price is not None:
            pnl_ratio = (
                ((float(candle.close) - float(entry_price)) / float(entry_price))
                if float(entry_price) > 0
                else 0.0
            )
            open_trade_path.append(
                {
                    "ts": int(candle.ts),
                    "close": float(candle.close),
                    "unrealized_pnl": (float(candle.close) - float(entry_price)) * float(qty),
                    "unrealized_pnl_pct": pnl_ratio * 100.0,
                }
            )
        pending_buy_qty = sum(item.qty for item in pending_fills if item.side == "BUY")
        pending_sell_qty = sum(item.qty for item in pending_fills if item.side == "SELL")
        sellable_qty = max(0.0, qty - pending_sell_qty)
        event_extra = event.extra_payload if isinstance(event.extra_payload, dict) else {}
        regime_snapshot = dict(
            event_extra.get("regime_snapshot")
            or {"composite_regime": "strategy_neutral_not_evaluated"}
        )
        regime_coverage_accumulator.update(regime_snapshot)
        if accumulator.retain_full_detail():
            regime_snapshots.append(regime_snapshot)
        entry_decision = event_extra.get("entry_decision")
        raw_signal = str(event.raw_signal or "HOLD").upper()
        raw_reason = str(event_extra.get("raw_reason") or event.reason)
        raw_filter_would_block = bool(event_extra.get("raw_filter_would_block", bool(event.blocked_filters)))
        entry_filter_blocked = bool(event_extra.get("entry_filter_blocked", False))
        entry_signal = str(event.entry_signal or raw_signal).upper()
        policy_position = _research_position_snapshot(
            qty=float(qty),
            sellable_qty=float(sellable_qty),
            pending_buy_qty=float(pending_buy_qty),
            pending_sell_qty=float(pending_sell_qty),
            entry_ts=entry_ts,
            entry_price=entry_price,
            candle_ts=int(candle.ts),
            market_price=float(candle.close),
        )
        evaluates_exit_policy = bool(
            isinstance(event.exit_intent, dict)
            and str(event.exit_intent.get("mode") or "") == "evaluate_exit_policy"
        )
        policy_builder_kwargs = {
            "event": event,
            "dataset": dataset,
            "candle_index": index,
            "position": policy_position,
            "parameter_values": parameter_values,
            "fee_rate": fee_rate,
            "slippage_bps": slippage_bps,
            "active_exit_policy": active_exit_policy,
            "buy_fraction": float(buy_fraction),
        }
        policy_materialization_mode = str(
            getattr(run_context, "policy_materialization_mode", "research_exploratory")
        )
        promotion_grade_policy_required = policy_materialization_mode != "research_exploratory"
        if strategy_plugin.policy_assembly_factory is not None:
            policy_builder_kwargs.update(
                {
                    "materialization_mode": policy_materialization_mode,
                    "candidate_regime_policy": (
                        dict(getattr(run_context, "candidate_regime_policy"))
                        if isinstance(getattr(run_context, "candidate_regime_policy", None), dict)
                        else None
                    ),
                    "candidate_regime_policy_enforced": bool(
                        getattr(run_context, "candidate_regime_policy_drives_research_execution", True)
                    ),
                }
            )
        policy_decision = (
            strategy_plugin.research_policy_decision_builder(**policy_builder_kwargs)
            if strategy_plugin.research_policy_decision_builder is not None
            else None
        )
        policy_unsupported_reason = ""
        allows_legacy_event_first_exit_policy = "research_runtime_contract.v2" not in str(event.strategy_version or "")
        if (
            strategy_plugin.research_policy_decision_builder is not None
            and policy_decision is None
            and not (evaluates_exit_policy and allows_legacy_event_first_exit_policy)
        ):
            policy_unsupported_reason = "research_policy_decision_missing_not_comparable"
        if promotion_grade_policy_required and policy_decision is None:
            raise ValueError(policy_unsupported_reason or "research_policy_decision_missing_not_comparable")
        if policy_decision is not None:
            entry_decision = policy_decision.entry_decision
            raw_signal = str(policy_decision.raw_signal or "HOLD").upper()
            raw_reason = str(policy_decision.raw_reason or raw_reason)
            raw_filter_would_block = bool(policy_decision.trace.get("raw_filter_would_block"))
            entry_filter_blocked = bool(policy_decision.trace.get("entry_blocked"))
            entry_signal = str(policy_decision.entry_signal or raw_signal).upper()
            exit_signal = str(policy_decision.exit_signal or raw_signal).upper()
            blocked_filters = list(policy_decision.blocked_filters)
        else:
            exit_signal = str(event.exit_signal or event.raw_signal or "HOLD").upper()
            blocked_filters = list(event.blocked_filters)
        market_regime_decision = (
            dict(getattr(entry_decision, "candidate_regime_decision"))
            if entry_decision is not None
            and isinstance(getattr(entry_decision, "candidate_regime_decision", None), dict)
            else {"regime_decision": "not_configured"}
        )
        market_regime_blocked = bool(
            getattr(entry_decision, "market_regime_triggered", False)
            if entry_decision is not None
            else False
        )
        candidate_regime_blocked = bool(
            getattr(entry_decision, "candidate_regime_triggered", False)
            if entry_decision is not None
            else False
        )
        policy_drives_execution = True
        if policy_decision is not None and policy_drives_execution:
            requested_action = str(policy_decision.final_signal or "HOLD").upper()
        elif policy_unsupported_reason:
            requested_action = "HOLD"
        else:
            requested_action = str(event.final_signal or "HOLD").upper()
        execution_policy_decision = policy_decision if policy_drives_execution else None
        action = requested_action
        blocked = bool(policy_unsupported_reason)
        block_reason = (
            str(policy_decision.final_reason)
            if policy_decision is not None and policy_drives_execution
            else policy_unsupported_reason or event.reason
        )
        exit_evaluations: list[dict[str, object]] = []
        exit_rule = str((event.exit_intent or {}).get("exit_rule") or "") if event.exit_intent else ""
        exit_reason = str((event.exit_intent or {}).get("exit_reason") or "") if event.exit_intent else ""
        if (
            evaluates_exit_policy
            and policy_decision is None
            and not policy_unsupported_reason
        ):
            action = "BUY" if requested_action == "BUY" else "HOLD"
            if sellable_qty > 1e-12:
                position = support.ResearchPositionContext(
                    in_position=True,
                    entry_ts=entry_ts,
                    entry_price=entry_price,
                    qty_open=sellable_qty,
                    holding_time_sec=(
                        max(0.0, (int(candle.ts) - int(entry_ts)) / 1000.0)
                        if entry_ts is not None
                        else 0.0
                    ),
                    unrealized_pnl=(
                        (float(candle.close) - float(entry_price)) * sellable_qty
                        if entry_price is not None
                        else 0.0
                    ),
                    unrealized_pnl_ratio=(
                        ((float(candle.close) - float(entry_price)) / float(entry_price))
                        if entry_price not in (None, 0.0)
                        else 0.0
                    ),
                )
                common_exit_rules = support.create_exit_rules(
                    rule_names=list(active_exit_policy.get("common_rules") or ()),
                    stop_loss_ratio=float(active_exit_policy.get("stop_loss", {}).get("stop_loss_ratio", 0.0)),
                    max_holding_sec=float(
                        active_exit_policy.get("max_holding_time", {}).get("max_holding_min", 0.0)
                    )
                    * 60.0,
                )
                strategy_exit_rules = []
                if strategy_plugin.exit_rule_factory is not None:
                    strategy_exit_rules = strategy_plugin.exit_rule_factory(
                        active_exit_policy,
                        parameter_values,
                        fee_rate,
                    )
                exit_rules = merge_exit_rules(common_exit_rules, strategy_exit_rules)
                common_exit_rule_names = {rule.name for rule in common_exit_rules}
                strategy_exit_rule_names = {rule.name for rule in strategy_exit_rules}
                for rule in exit_rules:
                    strategy_signal_context = (
                        strategy_plugin.exit_signal_context_builder(event)
                        if strategy_plugin.exit_signal_context_builder is not None
                        else {}
                    )
                    result = rule.evaluate(
                        position=position,
                        candle_ts=int(candle.ts),
                        market_price=float(candle.close),
                        signal_context={
                            "base_signal": raw_signal,
                            "base_reason": raw_reason,
                            "entry_signal": entry_signal,
                            "exit_signal": event.exit_signal or raw_signal,
                            **strategy_signal_context,
                        },
                    )
                    exit_evaluations.append(
                        {
                            "rule": rule.name,
                            "rule_source": _exit_rule_source(
                                rule_name=rule.name,
                                common_exit_rule_names=common_exit_rule_names,
                                strategy_exit_rule_names=strategy_exit_rule_names,
                            ),
                            "triggered": bool(result.should_exit),
                            "reason": result.reason,
                            "context": result.context,
                        }
                    )
                    if result.should_exit:
                        action = "SELL"
                        exit_rule = rule.name
                        exit_reason = result.reason
                        break
        if action == "BUY" and (qty > 1e-12 or pending_buy_qty > 1e-12):
            action = "HOLD"
            blocked = True
            block_reason = "buy_blocked_existing_position_or_pending_buy"
        elif action == "SELL" and sellable_qty <= 1e-12:
            action = "HOLD"
            blocked = True
            block_reason = "sell_blocked_no_sellable_qty"
        elif action not in {"BUY", "SELL", "HOLD"}:
            raise ValueError(f"unsupported_decision_event_final_signal:{event.final_signal}")
        allow_execution_compatibility_fallback = bool(
            policy_decision is None
            and not policy_unsupported_reason
            and (
                strategy_plugin.research_policy_decision_builder is None
                or allows_legacy_event_first_exit_policy
            )
        )
        execution_plan_bundle = _research_execution_plan_bundle(
            side=action,
            cash=float(cash),
            buy_fraction=float(buy_fraction),
            sellable_qty=float(sellable_qty),
            reference_price=float(candle.close),
            policy_decision=execution_policy_decision,
            candle_ts=int(candle.ts),
            allow_compatibility_fallback=(
                allow_execution_compatibility_fallback or not policy_drives_execution
            ),
            promotion_grade_required=(
                policy_drives_execution and not allow_execution_compatibility_fallback
            ),
            block_reason=block_reason,
        )
        submit_plan = execution_plan_bundle.submit_plan
        if policy_decision is not None:
            exit_evaluations = [dict(item) for item in policy_decision.exit_evaluations]
            exit_rule = str(policy_decision.exit_rule or "")
            exit_reason = policy_decision.exit_reason
            protective_exit_overrode_entry = bool(policy_decision.protective_exit_overrode_entry)
            entry_blocked = bool(policy_decision.entry_blocked)
            exit_filter_suppression_prevented = bool(
                policy_decision.exit_filter_suppression_prevented
            )
        elif policy_unsupported_reason:
            protective_exit_overrode_entry = False
            entry_blocked = False
            exit_filter_suppression_prevented = False
        else:
            protective_exit_overrode_entry = bool(
                raw_signal == "BUY"
                and action == "SELL"
                and exit_rule in {"stop_loss", "max_holding_time"}
            )
            entry_blocked = bool(raw_signal == "BUY" and action == "HOLD" and raw_filter_would_block)
            exit_filter_suppression_prevented = bool(
                raw_signal == "SELL"
                and raw_filter_would_block
                and sellable_qty > 1e-12
                and bool(exit_evaluations)
            )
        decision_payload = support.research_decision_payload(
            dataset=dataset,
            dataset_content_hash=dataset_content_hash,
            parameter_values=parameter_values,
            strategy_name=strategy_plugin.name,
            strategy_spec=strategy_spec.as_dict(),
            strategy_spec_hash=strategy_spec.spec_hash(),
            strategy_plugin_contract=strategy_plugin.contract_payload(),
            strategy_plugin_contract_hash=strategy_plugin.contract_hash(),
            exit_policy=active_exit_policy,
            exit_policy_hash=active_exit_policy_hash,
            fee_rate=fee_rate,
            slippage_bps=slippage_bps,
            timing_policy=timing_policy,
            portfolio_policy=policy,
            candle_ts=event.candle_ts,
            decision_ts=decision_boundary_ts,
            raw_signal=raw_signal,
            entry_signal=entry_signal,
            exit_signal=exit_signal,
            final_signal=action,
            raw_reason=raw_reason,
            blocked=bool(blocked or (raw_signal in {"BUY", "SELL"} and action == "HOLD")),
            raw_filter_would_block=raw_filter_would_block,
            entry_blocked=entry_blocked,
            protective_exit_overrode_entry=protective_exit_overrode_entry,
            exit_filter_suppression_prevented=exit_filter_suppression_prevented,
            blocked_filters=blocked_filters,
            feature_snapshot=dict(event.feature_snapshot),
            regime_snapshot=regime_snapshot,
            entry_reason=block_reason,
            market_regime_decision=market_regime_decision,
            market_regime_blocked=market_regime_blocked,
            candidate_regime_blocked=candidate_regime_blocked,
            qty=qty,
            sellable_qty=sellable_qty,
            exit_rule=exit_rule,
            exit_reason=exit_reason,
            exit_evaluations=exit_evaluations,
        )
        if strategy_plugin.decision_payload_adapter is not None:
            decision_payload = strategy_plugin.decision_payload_adapter(decision_payload, event)
        decision_payload.update(
            {
                "decision_event_schema_version": 1,
                "strategy_decision_contract_version": strategy_plugin.decision_contract_version,
                "raw_reason": raw_reason,
                "feature_snapshot": dict(event.feature_snapshot),
                "strategy_diagnostics_namespace": strategy_plugin.diagnostics_namespace,
                "strategy_diagnostics": dict(event.strategy_diagnostics),
                "strategy_behavior_payload": {
                    "strategy_name": event.strategy_name,
                    "strategy_version": event.strategy_version,
                    "raw_signal": raw_signal,
                    "final_signal": action,
                    "reason": block_reason,
                    "feature_snapshot": dict(event.feature_snapshot),
                    "strategy_diagnostics": dict(event.strategy_diagnostics),
                },
                "execution_intent": action.lower() if action in {"BUY", "SELL"} else "none",
                "order_intent": dict(event.order_intent) if event.order_intent is not None else None,
                "exit_intent": dict(event.exit_intent) if event.exit_intent is not None else None,
                "research_policy_position_terminal_state": policy_position.terminal_state,
                "research_policy_recomputed_with_simulated_position": policy_decision is not None,
                "research_policy_unsupported": bool(policy_unsupported_reason),
                "research_policy_unsupported_reason": policy_unsupported_reason,
                "research_policy_comparable": not bool(policy_unsupported_reason),
                **_execution_plan_evidence(execution_plan_bundle),
            }
        )
        if policy_decision is not None:
            decision_payload["pure_policy_hash"] = policy_decision.policy_hash
            decision_payload["policy_contract_hash"] = policy_decision.policy_contract_hash
            decision_payload["policy_input_hash"] = policy_decision.policy_input_hash
            decision_payload["policy_decision_hash"] = policy_decision.policy_decision_hash
            decision_payload["pure_policy_trace"] = policy_decision.as_trace()
            trace = policy_decision.as_trace()
            service_provenance = trace.get("strategy_evaluation_provenance")
            if isinstance(service_provenance, dict):
                decision_payload["strategy_evaluation_provenance"] = dict(service_provenance)
            decision_payload["execution_intent_v2"] = (
                policy_decision.execution_intent.as_dict()
                if policy_decision.execution_intent is not None
                else None
            )
            diagnostics = (
                dict(decision_payload["strategy_diagnostics"])
                if isinstance(decision_payload.get("strategy_diagnostics"), dict)
                else {}
            )
            diagnostics.update(
                {
                    "pure_policy_hash": policy_decision.policy_hash,
                    "policy_contract_hash": policy_decision.policy_contract_hash,
                    "policy_input_hash": policy_decision.policy_input_hash,
                    "policy_decision_hash": policy_decision.policy_decision_hash,
                    "pure_policy_trace": policy_decision.as_trace(),
                    "policy_position_terminal_state": policy_position.terminal_state,
                    "policy_recomputed_with_simulated_position": True,
                }
            )
            decision_payload["strategy_diagnostics"] = diagnostics
        retain_decision = accumulator.retain_decision()
        if retain_decision:
            decisions.append(decision_payload)
        accumulator.update_decision(decision_payload, retained=retain_decision)
        support.trace_decision(run_context, decision_payload)

        if action in {"BUY", "SELL"}:
            if submit_plan is None:
                warnings.append("research_submit_plan_missing")
                continue
            side = action
            signal = build_signal_event(
                candle=candle,
                interval=dataset.interval,
                side=side,
                policy=timing_policy,
                feature_snapshot=dict(event.feature_snapshot),
                regime_snapshot=regime_snapshot,
            )
            reference = resolve_execution_reference(
                dataset=dataset,
                signal=signal,
                signal_index=index,
                policy=timing_policy,
                model_latency_ms=support.model_latency_ms(model),
            )
            execution_service = ResearchVirtualExecutionService(
                execution_model=model,
                fee_rate=fee_rate,
            )
            timing_fields = support.timing_request_fields(signal, reference, timing_policy)
            depth_fields = support.depth_request_fields(
                dataset=dataset,
                reference=reference,
                model=model,
                timing_policy=timing_policy,
            )
            research_execution_context = ResearchExecutionContext(
                signal_ts=signal.signal_candle_start_ts,
                decision_ts=signal.decision_ts,
                timing_fields=timing_fields,
                depth_fields=depth_fields,
            )
            if reference.fill_reference_price is None:
                fill = support.failed_fill(
                    model=model,
                    signal=signal,
                    reference=reference,
                    timing_policy=timing_policy,
                    side=side,
                    fee_rate=fee_rate,
                    requested_qty=_positive_float_or_none(submit_plan.qty),
                    requested_notional=_positive_float_or_none(submit_plan.notional_krw),
                )
            else:
                try:
                    fill = execution_service.execute(
                        SignalExecutionRequest(
                            signal=side,
                            ts=signal.signal_candle_start_ts,
                            market_price=float(reference.fill_reference_price),
                            strategy_name=strategy_plugin.name,
                            decision_reason=block_reason,
                            execution_decision_summary=execution_plan_bundle.summary,
                            execution_plan_bundle=execution_plan_bundle,
                            research_execution_context=research_execution_context,
                        ),
                    )
                except ValueError as exc:
                    warnings.append(f"research_typed_execution_service_failed:{exc}")
                    continue
                if fill is None:
                    warnings.append(
                        f"research_typed_execution_service_no_fill:{submit_plan.block_reason or 'none'}"
                    )
                    continue
            warnings.extend(support.execution_reference_warnings(fill))
            if fill.fill_status == "failed" or fill.avg_fill_price is None or fill.filled_qty <= 0.0:
                trades.append(support.trade_from_fill(fill, cash=cash, asset_qty=qty, pnl=None))
                support.trace_execution(run_context, trades[-1])
            elif side == "BUY":
                exec_price = float(fill.avg_fill_price)
                fee = float(fill.fee)
                received_qty = float(fill.filled_qty)
                actual_spend = (exec_price * received_qty) + fee
                buy_slippage = max(0.0, (exec_price - float(fill.reference_price)) * received_qty)
                pending = support.PendingFill(
                    fill=fill,
                    trade_index=len(trades),
                    side="BUY",
                    effective_ts=support.fill_effective_ts(fill),
                    qty=received_qty,
                    fee=fee,
                    slippage=buy_slippage,
                    cash_delta=-actual_spend,
                    entry_regime_snapshot=regime_snapshot,
                )
                trades.append(support.pending_trade_from_fill(fill, cash=cash, asset_qty=qty))
                trades[-1]["entry_decision_hash"] = decision_payload.get("replay_fingerprint_hash")
                support.trace_execution(run_context, trades[-1])
                if support.fill_applies_to_mark(fill=pending.fill, effective_ts=pending.effective_ts, mark_boundary_ts=mark_boundary_ts):
                    mark_cash += pending.cash_delta
                    mark_qty += pending.qty
                pending_fills.append(pending)
            else:
                exec_price = float(fill.avg_fill_price)
                sell_qty = float(fill.filled_qty)
                gross = sell_qty * exec_price
                fee = float(fill.fee)
                sell_slippage = max(0.0, (float(fill.reference_price) - exec_price) * sell_qty)
                pending = support.PendingFill(
                    fill=fill,
                    trade_index=len(trades),
                    side="SELL",
                    effective_ts=support.fill_effective_ts(fill),
                    qty=sell_qty,
                    fee=fee,
                    slippage=sell_slippage,
                    cash_delta=gross - fee,
                    entry_regime_snapshot=entry_regime_snapshot,
                    exit_regime_snapshot=regime_snapshot,
                )
                trades.append(support.pending_trade_from_fill(fill, cash=cash, asset_qty=qty))
                trades[-1].update(
                    support.closed_trade_diagnostics(
                        entry_ts=entry_ts,
                        exit_ts=int(candle.ts),
                        entry_price=entry_price,
                        exit_price=exec_price,
                        entry_regime_snapshot=entry_regime_snapshot,
                        exit_regime_snapshot=regime_snapshot,
                        exit_rule=exit_rule,
                        exit_reason=exit_reason,
                        path=open_trade_path,
                        entry_decision_hash=entry_decision_hash,
                        exit_decision_hash=str(decision_payload.get("replay_fingerprint_hash") or ""),
                    )
                )
                support.trace_execution(run_context, trades[-1])
                if support.fill_applies_to_mark(fill=pending.fill, effective_ts=pending.effective_ts, mark_boundary_ts=mark_boundary_ts):
                    mark_cash += pending.cash_delta
                    mark_qty = max(0.0, mark_qty - pending.qty)
                pending_fills.append(pending)
            (
                cash,
                qty,
                entry_cost_basis,
                entry_regime_snapshot,
                entry_ts,
                entry_price,
                entry_decision_hash,
                open_trade_path,
                entry_fee,
                entry_slippage,
                fee_total,
                slippage_total,
            ) = support.apply_pending_fills(
                pending_fills=pending_fills,
                trades=trades,
                boundary_ts=decision_boundary_ts,
                cash=cash,
                qty=qty,
                entry_cost_basis=entry_cost_basis,
                entry_regime_snapshot=entry_regime_snapshot,
                entry_ts=entry_ts,
                entry_price=entry_price,
                entry_decision_hash=entry_decision_hash,
                open_trade_path=open_trade_path,
                entry_fee=entry_fee,
                entry_slippage=entry_slippage,
                fee_total=fee_total,
                slippage_total=slippage_total,
                closed_pnls=closed_pnls,
            )

        retain_equity = accumulator.retain_equity_point()
        peak, max_drawdown = support.record_equity_mark(
            equity_curve=equity_curve,
            ts=mark_boundary_ts,
            cash=mark_cash,
            qty=mark_qty,
            mark_price=candle.close,
            peak=peak,
            max_drawdown=max_drawdown,
            retain=retain_equity,
        )
        accumulator.update_equity(retained=retain_equity, ts=mark_boundary_ts, asset_qty=mark_qty)
        support.trace_equity_mark(
            run_context,
            ts=mark_boundary_ts,
            equity=mark_cash + mark_qty * float(candle.close),
            cash=mark_cash,
            asset_qty=mark_qty,
        )
        accumulator.maybe_emit_heartbeat(event_number)
        accumulator.check_limits(candles_processed=event_number, trades=trades)

    last = candles[-1]
    last_mark_ts = candle_close_ts(last, interval=dataset.interval)
    (
        cash,
        qty,
        entry_cost_basis,
        entry_regime_snapshot,
        entry_ts,
        entry_price,
        entry_decision_hash,
        open_trade_path,
        entry_fee,
        entry_slippage,
        fee_total,
        slippage_total,
    ) = support.apply_pending_fills(
        pending_fills=pending_fills,
        trades=trades,
        boundary_ts=last_mark_ts,
        cash=cash,
        qty=qty,
        entry_cost_basis=entry_cost_basis,
        entry_regime_snapshot=entry_regime_snapshot,
        entry_ts=entry_ts,
        entry_price=entry_price,
        entry_decision_hash=entry_decision_hash,
        open_trade_path=open_trade_path,
        entry_fee=entry_fee,
        entry_slippage=entry_slippage,
        fee_total=fee_total,
        slippage_total=slippage_total,
        closed_pnls=closed_pnls,
    )

    support.mark_pending_fills_at_end(pending_fills=pending_fills, trades=trades, final_mark_ts=last_mark_ts)
    final_equity = cash + qty * float(last.close)
    retain_final_equity = accumulator.retain_equity_point()
    if retain_final_equity:
        equity_curve.append(EquityPoint(ts=last_mark_ts, equity=final_equity, cash=cash, asset_qty=qty))
    accumulator.update_equity(retained=retain_final_equity, ts=last_mark_ts, asset_qty=qty)
    support.trace_equity_mark(run_context, ts=last_mark_ts, equity=final_equity, cash=cash, asset_qty=qty)
    return_pct = ((final_equity / starting_cash) - 1.0) * 100.0 if starting_cash > 0.0 else 0.0
    metrics = support.metrics(
        return_pct=return_pct,
        max_drawdown_pct=max_drawdown * 100.0,
        closed_pnls=closed_pnls,
        fee_total=fee_total,
        slippage_total=slippage_total,
        parameter_stability_score=parameter_stability_score,
    )
    position_intervals, closed_trade_records, execution_records, derived_open_cost_basis = support.metrics_v2_ledgers_from_trades(
        trades=trades,
    )
    coverage = (
        aggregate_regime_coverage(snapshots=regime_snapshots, trades=trades)
        if accumulator.retain_full_detail()
        else regime_coverage_accumulator.coverage(trades=trades)
    )
    performance = aggregate_regime_performance(trades=trades, coverage=coverage, start_cash=starting_cash)
    metrics_v2 = build_metrics_v2(
        starting_cash=starting_cash,
        final_cash=cash,
        final_asset_qty=qty,
        final_mark_price=last.close,
        final_open_cost_basis=entry_cost_basis if qty > 0.0 else derived_open_cost_basis,
        equity_curve=tuple(equity_curve),
        position_intervals=position_intervals,
        closed_trades=closed_trade_records,
        execution_records=execution_records,
        **(
            {}
            if accumulator.retain_full_detail()
            else accumulator.metrics_summary_inputs(max_drawdown_pct=max_drawdown * 100.0)
        ),
    )
    if not accumulator.retain_full_detail():
        metrics_v2 = replace(
            metrics_v2,
            limitation_reasons=tuple(
                sorted(set(metrics_v2.limitation_reasons) | {"bounded_detail_equity_curve_not_retained"})
            ),
        )
    audit_trace_index = support.complete_audit_trace(run_context, status="completed")
    accumulator.trade_ledger_hash_material = [support.trade_hash_payload(trade) for trade in trades]
    accumulator.equity_curve_hash_material = [
        {
            "ts": int(point.ts),
            "equity": round(float(point.equity), 12),
            "cash": round(float(point.cash), 12),
            "asset_qty": round(float(point.asset_qty), 12),
        }
        for point in equity_curve
    ]
    strategy_diagnostics = accumulator.strategy_diagnostics(trades=trades)
    resource_usage = accumulator.resource_usage(candles_processed=len(decision_events))
    resource_usage["strategy_diagnostics"] = strategy_diagnostics
    return BacktestRun(
        metrics=metrics,
        metrics_v2=metrics_v2,
        trades=tuple(trades),
        candle_count=len(candles),
        warnings=tuple(warnings),
        regime_performance=performance,
        regime_coverage=coverage,
        execution_event_summary=execution_event_summary(trades),
        decisions=tuple(decisions),
        equity_curve=tuple(equity_curve),
        position_intervals=position_intervals,
        closed_trades=closed_trade_records,
        resource_usage=resource_usage,
        strategy_diagnostics=strategy_diagnostics,
        retained_detail_summary=support.retained_detail_summary(
            accumulator,
            retained_regime_snapshot_count=len(regime_snapshots),
        ),
        audit_trace_index=audit_trace_index,
    )


def _exit_rule_source(
    *,
    rule_name: str,
    common_exit_rule_names: set[str],
    strategy_exit_rule_names: set[str],
) -> str:
    in_common = rule_name in common_exit_rule_names
    in_strategy = rule_name in strategy_exit_rule_names
    if in_common and in_strategy:
        return "common_risk_and_plugin"
    if in_common:
        return "common_risk"
    if in_strategy:
        return "plugin"
    return "unknown"
