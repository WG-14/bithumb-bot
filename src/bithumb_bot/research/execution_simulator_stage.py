from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from . import backtest_support as support
from .backtest_loop import ResearchExecutionPlanBundle
from .backtest_loop import _execution_plan_evidence as _default_execution_plan_evidence
from .backtest_loop import _research_execution_plan_bundle as _default_research_execution_plan_bundle
from .execution_simulator import ResearchExecutionContext, ResearchVirtualExecutionService
from .execution_timing import build_signal_event, resolve_execution_reference


@dataclass(frozen=True)
class ExecutionSimulationOutcome:
    fill: Any | None
    pending_fill: support.PendingFill | None = None
    trade: dict[str, object] | None = None
    warnings: tuple[str, ...] = ()
    plan_bundle: Any | None = None
    evidence: dict[str, object] = field(default_factory=dict)
    mark_cash_delta: float = 0.0
    mark_qty_delta: float = 0.0


@dataclass(frozen=True)
class DefaultExecutionSimulator:
    """Typed execution planning and virtual fill authority boundary."""

    def run(self, state: Any) -> Any:
        return state

    def execute(self, *args: Any, **kwargs: Any) -> ExecutionSimulationOutcome:
        from bithumb_bot.execution_service import SignalExecutionRequest

        action = str(kwargs["action"]).upper()
        if action not in {"BUY", "SELL"}:
            return ExecutionSimulationOutcome(fill=None)
        dataset = kwargs["dataset"]
        candle = kwargs["candle"]
        index = int(kwargs["candle_index"])
        event = kwargs["event"]
        ledger = kwargs["ledger"]
        timing_policy = kwargs["timing_policy"]
        model = kwargs["execution_model"]
        fee_rate = float(kwargs["fee_rate"])
        strategy_name = str(kwargs["strategy_name"])
        decision_reason = str(kwargs["decision_reason"])
        regime_snapshot = dict(kwargs["regime_snapshot"])
        decision_hash = str(kwargs.get("decision_hash") or "")
        sellable_qty = float(kwargs["sellable_qty"])
        buy_fraction = float(kwargs["buy_fraction"])
        promotion_grade_policy_required = bool(kwargs["promotion_grade_policy_required"])
        allow_execution_compatibility_fallback = bool(kwargs["allow_execution_compatibility_fallback"])
        policy_drives_execution = bool(kwargs.get("policy_drives_execution", True))
        policy_decision = kwargs.get("policy_decision")
        plan_bundle_builder = _compat_attr("_research_execution_plan_bundle", _default_research_execution_plan_bundle)
        plan_bundle = plan_bundle_builder(
            side=action,
            cash=float(ledger.cash),
            buy_fraction=buy_fraction,
            sellable_qty=sellable_qty,
            reference_price=float(candle.close),
            policy_decision=policy_decision if policy_drives_execution else None,
            candle_ts=int(candle.ts),
            allow_compatibility_fallback=(
                allow_execution_compatibility_fallback or not policy_drives_execution
            ),
            promotion_grade_required=(
                policy_drives_execution
                and promotion_grade_policy_required
                and not allow_execution_compatibility_fallback
            ),
            block_reason=decision_reason,
        )
        submit_plan = plan_bundle.submit_plan
        evidence_builder = _compat_attr("_execution_plan_evidence", _default_execution_plan_evidence)
        evidence = evidence_builder(plan_bundle)
        if submit_plan is None:
            if promotion_grade_policy_required:
                raise ValueError("research_submit_plan_missing")
            return ExecutionSimulationOutcome(
                fill=None,
                plan_bundle=plan_bundle,
                evidence=evidence,
                warnings=("research_submit_plan_missing",),
            )
        signal = build_signal_event(
            candle=candle,
            interval=dataset.interval,
            side=action,
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
                side=action,
                fee_rate=fee_rate,
                requested_qty=_positive_float_or_none(submit_plan.qty),
                requested_notional=_positive_float_or_none(submit_plan.notional_krw),
            )
        else:
            service_cls = _compat_attr("ResearchVirtualExecutionService", ResearchVirtualExecutionService)
            service = service_cls(execution_model=model, fee_rate=fee_rate)
            fill = service.execute(
                SignalExecutionRequest(
                    signal=action,
                    ts=signal.signal_candle_start_ts,
                    market_price=float(reference.fill_reference_price),
                    strategy_name=strategy_name,
                    decision_reason=decision_reason,
                    execution_decision_summary=plan_bundle.summary,
                    execution_plan_bundle=plan_bundle,
                    research_execution_context=research_execution_context,
                )
            )
            if fill is None:
                return ExecutionSimulationOutcome(
                    fill=None,
                    plan_bundle=plan_bundle,
                    evidence=evidence,
                    warnings=(f"research_typed_execution_service_no_fill:{submit_plan.block_reason or 'none'}",),
                )
        warnings = tuple(support.execution_reference_warnings(fill))
        if fill.fill_status == "failed" or fill.avg_fill_price is None or fill.filled_qty <= 0.0:
            return ExecutionSimulationOutcome(
                fill=fill,
                plan_bundle=plan_bundle,
                evidence=evidence,
                warnings=warnings,
                trade=support.trade_from_fill(fill, cash=ledger.cash, asset_qty=ledger.qty, pnl=None),
            )
        if action == "BUY":
            exec_price = float(fill.avg_fill_price)
            fee = float(fill.fee)
            received_qty = float(fill.filled_qty)
            actual_spend = (exec_price * received_qty) + fee
            buy_slippage = max(0.0, (exec_price - float(fill.reference_price)) * received_qty)
            pending = support.PendingFill(
                fill=fill,
                trade_index=len(ledger.trade_ledger),
                side="BUY",
                effective_ts=support.fill_effective_ts(fill),
                qty=received_qty,
                fee=fee,
                slippage=buy_slippage,
                cash_delta=-actual_spend,
                entry_regime_snapshot=regime_snapshot,
            )
            trade = support.pending_trade_from_fill(fill, cash=ledger.cash, asset_qty=ledger.qty)
            trade["entry_decision_hash"] = decision_hash
            return ExecutionSimulationOutcome(
                fill=fill,
                pending_fill=pending,
                trade=trade,
                warnings=warnings,
                plan_bundle=plan_bundle,
                evidence=evidence,
                mark_cash_delta=pending.cash_delta,
                mark_qty_delta=pending.qty,
            )
        exec_price = float(fill.avg_fill_price)
        sell_qty = float(fill.filled_qty)
        gross = sell_qty * exec_price
        fee = float(fill.fee)
        sell_slippage = max(0.0, (float(fill.reference_price) - exec_price) * sell_qty)
        pending = support.PendingFill(
            fill=fill,
            trade_index=len(ledger.trade_ledger),
            side="SELL",
            effective_ts=support.fill_effective_ts(fill),
            qty=sell_qty,
            fee=fee,
            slippage=sell_slippage,
            cash_delta=gross - fee,
            entry_regime_snapshot=ledger.entry_regime_snapshot,
            exit_regime_snapshot=regime_snapshot,
        )
        trade = support.pending_trade_from_fill(fill, cash=ledger.cash, asset_qty=ledger.qty)
        trade.update(
            support.closed_trade_diagnostics(
                entry_ts=ledger.entry_ts,
                exit_ts=int(candle.ts),
                entry_price=ledger.entry_price,
                exit_price=exec_price,
                entry_regime_snapshot=ledger.entry_regime_snapshot,
                exit_regime_snapshot=regime_snapshot,
                exit_rule=str(kwargs.get("exit_rule") or ""),
                exit_reason=str(kwargs.get("exit_reason") or ""),
                path=ledger.open_trade_path,
                entry_decision_hash=ledger.entry_decision_hash,
                exit_decision_hash=decision_hash,
            )
        )
        return ExecutionSimulationOutcome(
            fill=fill,
            pending_fill=pending,
            trade=trade,
            warnings=warnings,
            plan_bundle=plan_bundle,
            evidence=evidence,
            mark_cash_delta=pending.cash_delta,
            mark_qty_delta=-pending.qty,
        )


def blocked_execution_evidence(reason_code: str) -> dict[str, object]:
    evidence_builder = _compat_attr("_execution_plan_evidence", _default_execution_plan_evidence)
    return evidence_builder(
        ResearchExecutionPlanBundle(
            submit_plan=None,
            summary=None,
            source="research_backtest",
            authority="research_virtual_execution_planner",
            execution_engine="research_virtual",
            status="BLOCKED",
            reason_code=str(reason_code),
        )
    )


def _positive_float_or_none(value: object) -> float | None:
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0.0 else None


def _compat_attr(name: str, default: Any) -> Any:
    try:
        from . import backtest_pipeline
    except Exception:
        return default
    return getattr(backtest_pipeline, name, default)
