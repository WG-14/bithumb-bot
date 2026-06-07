from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from . import backtest_support as support
from bithumb_bot.canonical_decision import canonical_payload_hash

from .artifact_store import ArtifactBudgetExceeded
from .audit_trace_recorder import AuditTraceRecorder
from .backtest_result_assembler import BacktestResultAssembler
from .backtest_stages import (
    ExecutionStageResult,
    LedgerStageResult,
    ObservabilityStageResult,
    ReplayTick,
    RiskStageResult,
    StrategyStageResult,
)
from .decision_payload import DecisionPayloadBuilder
from .execution_planner_stage import ExecutionPlanningRequest
from .execution_simulator_stage import ExecutionSimulationRequest, blocked_execution_evidence
from .execution_model import FixedBpsExecutionModel
from .execution_timing import candle_close_ts
from .experiment_manifest import ExecutionTimingPolicy, legacy_research_portfolio_policy
from .metrics_contract import EquityPoint
from .portfolio_ledger import PortfolioLedger
from .risk_gate_stage import PortfolioRiskSnapshot, RiskContextBuilder
from .stage_trace_recorder import StageTraceRecorder
from .strategy_spec import (
    exit_policy_hash,
    exit_policy_materialization_from_parameters,
    strategy_spec_for_name,
)


@dataclass(frozen=True)
class BacktestEventProcessResult:
    mark_boundary_ts: int
    mark_cash: float
    mark_qty: float
    retained_equity: bool
    decision_payload: dict[str, object]


@dataclass(frozen=True)
class TickPreparation:
    tick: ReplayTick
    mark_boundary_ts: int
    decision_boundary_ts: int
    mark_cash: float
    mark_qty: float
    sellable_qty: float
    portfolio_snapshot: dict[str, object]
    regime_snapshot: dict[str, object]
    position_snapshot: Any


@dataclass
class BacktestEventProcessor:
    """Coordinates one replay tick through the stage-owned authority path."""

    dataset: Any
    strategy_name: str
    parameter_values: dict[str, Any]
    fee_rate: float
    slippage_bps: float
    timing_policy: Any
    execution_model: Any
    portfolio_policy: Any
    risk_policy: Any
    strategy_plugin: Any
    strategy_spec: Any
    active_exit_policy: dict[str, Any]
    active_exit_policy_hash: str
    active_exit_policy_config_hash: str
    buy_fraction: float
    run_context: Any
    ledger: PortfolioLedger
    accumulator: Any
    payload_builder: DecisionPayloadBuilder
    audit_recorder: AuditTraceRecorder
    trace_recorder: StageTraceRecorder
    strategy_evaluator: Any
    risk_gate: Any
    execution_planner: Any
    execution_simulator: Any
    metrics_collector: Any | None
    experiment_recorder: Any | None
    dataset_content_hash: str
    warnings: list[str]
    decisions: list[dict[str, object]]
    regime_snapshots: list[dict[str, object]]
    regime_coverage_accumulator: Any

    def process_tick(self, *, tick: ReplayTick, event_number: int) -> BacktestEventProcessResult:
        prepared = self._prepare_tick(tick)
        strategy = self._evaluate_strategy(prepared)
        risk = self._evaluate_risk(prepared, strategy)
        execution = self._execute(prepared, risk)
        ledger = self._mark_ledger(prepared, execution)
        observability = self._record_observability(prepared, ledger, event_number)
        return BacktestEventProcessResult(
            mark_boundary_ts=ledger.mark_boundary_ts,
            mark_cash=ledger.mark_cash,
            mark_qty=ledger.mark_qty,
            retained_equity=ledger.retained_equity,
            decision_payload=observability.decision_payload,
        )

    def _prepare_tick(self, tick: ReplayTick) -> TickPreparation:
        event = tick.event
        candle = tick.candle
        mark_boundary_ts = candle_close_ts(candle, interval=self.dataset.interval)
        decision_boundary_ts = int(event.decision_ts)
        tick_state = self.ledger.begin_tick(
            mark_boundary_ts=mark_boundary_ts,
            decision_boundary_ts=decision_boundary_ts,
            candle_ts=int(candle.ts),
            close=float(candle.close),
        )
        mark_cash = tick_state.mark_cash
        mark_qty = tick_state.mark_qty
        sellable_qty = tick_state.sellable_qty
        event_extra = event.extra_payload if isinstance(event.extra_payload, dict) else {}
        regime_snapshot = dict(
            event_extra.get("regime_snapshot")
            or {"composite_regime": "strategy_neutral_not_evaluated"}
        )
        self.regime_coverage_accumulator.update(regime_snapshot)
        if self.accumulator.retain_full_detail():
            self.regime_snapshots.append(regime_snapshot)

        policy_position = self.ledger.snapshot_for_policy(
            candle_ts=int(candle.ts),
            market_price=float(candle.close),
        )
        return TickPreparation(
            tick=tick,
            mark_boundary_ts=mark_boundary_ts,
            decision_boundary_ts=decision_boundary_ts,
            mark_cash=mark_cash,
            mark_qty=mark_qty,
            sellable_qty=sellable_qty,
            portfolio_snapshot={"qty": self.ledger.qty, **self.ledger.portfolio_snapshot(tick_state)},
            regime_snapshot=regime_snapshot,
            position_snapshot=policy_position,
        )

    def _evaluate_strategy(self, prepared: TickPreparation) -> StrategyStageResult:
        tick = prepared.tick
        event = tick.event
        replay_tick_hash = canonical_payload_hash(
            {
                "candle_ts": int(tick.candle_ts),
                "decision_ts": int(tick.decision_ts),
                "raw_signal": event.raw_signal,
                "final_signal": event.final_signal,
                "reason": event.reason,
            }
        )
        position_snapshot_hash = canonical_payload_hash(
            prepared.position_snapshot.as_dict()
            if hasattr(prepared.position_snapshot, "as_dict")
            else vars(prepared.position_snapshot)
        )
        strategy_envelope = self.strategy_evaluator.evaluate(
            tick,
            prepared.position_snapshot,
            {
                "dataset": self.dataset,
                "strategy_name": self.strategy_name,
                "parameter_values": self.parameter_values,
                "fee_rate": self.fee_rate,
                "slippage_bps": self.slippage_bps,
                "active_exit_policy": self.active_exit_policy,
                "buy_fraction": self.buy_fraction,
                "run_context": self.run_context,
            },
        )
        strategy_decision_hash = canonical_payload_hash(
            {
                "replay_fingerprint_hash": strategy_envelope.replay_fingerprint_hash,
                "compatibility_fallback": strategy_envelope.compatibility_fallback,
                "unsupported_reason": strategy_envelope.unsupported_reason,
                "decision_hash": (
                    getattr(strategy_envelope.decision, "policy_decision_hash", "")
                    if strategy_envelope.decision is not None
                    else ""
                ),
            }
        )
        self.trace_recorder.record_strategy(
            replay_tick_hash=replay_tick_hash,
            position_snapshot_hash=position_snapshot_hash,
            strategy_decision_hash=strategy_decision_hash,
            compatibility_fallback=bool(strategy_envelope.compatibility_fallback),
            unsupported_reason=strategy_envelope.unsupported_reason,
            recommended_next_action=strategy_envelope.recommended_next_action,
        )
        return StrategyStageResult(
            tick=tick,
            position_snapshot=prepared.position_snapshot,
            envelope=strategy_envelope,
            replay_tick_hash=replay_tick_hash,
            position_snapshot_hash=position_snapshot_hash,
            strategy_decision_hash=strategy_decision_hash,
        )

    def _evaluate_risk(
        self,
        prepared: TickPreparation,
        strategy: StrategyStageResult,
    ) -> RiskStageResult:
        tick = prepared.tick
        candle = tick.candle
        event = tick.event
        policy_decision = strategy.envelope.decision
        current_equity = float(prepared.mark_cash) + float(prepared.mark_qty) * float(candle.close)
        baseline_equity = float(self.ledger.starting_cash)
        risk_context = RiskContextBuilder().build(
            strategy_plugin=self.strategy_plugin,
            event=event,
            active_exit_policy=self.active_exit_policy,
            parameter_values=self.parameter_values,
            fee_rate=self.fee_rate,
            strategy_envelope=strategy.envelope,
            portfolio_risk_snapshot=PortfolioRiskSnapshot(
                current_equity=current_equity,
                baseline_equity=baseline_equity,
                loss_today=max(0.0, baseline_equity - current_equity),
                current_cash=float(prepared.mark_cash),
                current_asset_qty=float(prepared.mark_qty),
                position_entry_price=getattr(strategy.position_snapshot, "entry_price", None),
                risk_policy=self.risk_policy,
            ),
            evaluation_ts_ms=int(candle.ts),
            mark_price=float(candle.close),
        )
        risk_decision = self.risk_gate.evaluate(
            policy_decision,
            strategy.position_snapshot,
            {
                "candle_ts": int(candle.ts),
                "close": float(candle.close),
            },
            prepared.portfolio_snapshot,
            risk_context,
        )
        risk_gate_hash = risk_decision.evidence_hash
        self.trace_recorder.record_risk(
            input_hash=strategy.strategy_decision_hash,
            risk_gate_hash=risk_gate_hash,
            reason_code=risk_decision.reason_code,
            payload=risk_decision.payload or {},
        )
        return RiskStageResult(
            strategy=strategy,
            decision=risk_decision,
            risk_gate_hash=risk_gate_hash,
            final_signal=risk_decision.final_signal,
        )

    def _execute(self, prepared: TickPreparation, risk: RiskStageResult) -> ExecutionStageResult:
        tick = prepared.tick
        event = tick.event
        candle = tick.candle
        action = risk.final_signal
        risk_decision = risk.decision
        strategy_envelope = risk.strategy.envelope
        policy_decision = strategy_envelope.decision
        mark_cash = prepared.mark_cash
        mark_qty = prepared.mark_qty
        decision_payload_qty = float(self.ledger.qty)
        decision_payload_sellable_qty = float(prepared.sellable_qty)
        if action in {"BUY", "SELL"}:
            promotion_grade_policy_required = bool(
                strategy_envelope.provenance.get("promotion_grade_policy_required")
            )
            try:
                planning = self.execution_planner.plan(
                    ExecutionPlanningRequest(
                        candle=candle,
                        event=event,
                        ledger=self.ledger,
                        strategy_name=self.strategy_plugin.name,
                        action=action,
                        decision_reason=risk_decision.reason_code,
                        sellable_qty=prepared.sellable_qty,
                        buy_fraction=self.buy_fraction,
                        promotion_grade_policy_required=promotion_grade_policy_required,
                        allow_execution_compatibility_fallback=bool(
                            strategy_envelope.provenance.get("allow_execution_compatibility_fallback")
                        ),
                        policy_drives_execution=True,
                        policy_decision=policy_decision,
                    )
                )
            except ValueError as exc:
                planning_error = str(exc)
                if promotion_grade_policy_required or planning_error not in {
                    "research_submit_plan_missing",
                    "research_typed_submit_plan_missing",
                }:
                    raise
                self.warnings.append(planning_error)
                outcome = None
                execution_evidence = blocked_execution_evidence(planning_error)
                planning_hash = canonical_payload_hash(execution_evidence)
                self.trace_recorder.record_execution_planning(
                    input_hash=risk.risk_gate_hash,
                    execution_plan_hash=planning_hash,
                    reason_code=planning_error,
                )
                execution_plan_hash = planning_hash
                fill_hash = canonical_payload_hash({})
            else:
                self.warnings.extend(planning.warnings)
                planning_hash = canonical_payload_hash(planning.evidence)
                self.trace_recorder.record_execution_planning(
                    input_hash=risk.risk_gate_hash,
                    execution_plan_hash=planning_hash,
                    reason_code=str(
                        planning.evidence.get("execution_plan_reason_code") or risk_decision.reason_code
                    ),
                )
                outcome = self.execution_simulator.execute(
                    ExecutionSimulationRequest(
                        dataset=self.dataset,
                        candle=candle,
                        candle_index=int(tick.candle_index),
                        event=event,
                        ledger=self.ledger,
                        timing_policy=self.timing_policy,
                        execution_model=self.execution_model,
                        fee_rate=self.fee_rate,
                        strategy_name=self.strategy_plugin.name,
                        action=action,
                        decision_reason=risk_decision.reason_code,
                        regime_snapshot=prepared.regime_snapshot,
                        decision_hash=str(
                            strategy_envelope.replay_fingerprint_hash or risk.strategy.strategy_decision_hash
                        ),
                        sellable_qty=prepared.sellable_qty,
                        buy_fraction=self.buy_fraction,
                        promotion_grade_policy_required=promotion_grade_policy_required,
                        allow_execution_compatibility_fallback=bool(
                            strategy_envelope.provenance.get("allow_execution_compatibility_fallback")
                        ),
                        policy_drives_execution=True,
                        policy_decision=policy_decision,
                        plan_bundle=planning.plan_bundle,
                        execution_evidence=planning.evidence,
                        exit_rule=risk_decision.exit_rule,
                        exit_reason=risk_decision.exit_reason,
                    )
                )
                execution_evidence = dict(outcome.evidence)
                self.warnings.extend(outcome.warnings)
                application = self.ledger.apply_execution_outcome(
                    outcome,
                    mark_boundary_ts=prepared.mark_boundary_ts,
                    mark_cash=prepared.mark_cash,
                    mark_qty=prepared.mark_qty,
                )
                mark_cash = application.mark_cash
                mark_qty = application.mark_qty
                if application.trade_recorded:
                    _record_audit_execution(
                        self.audit_recorder,
                        self.run_context,
                        self.warnings,
                        self.trace_recorder,
                        input_hash=canonical_payload_hash(self.ledger.trade_ledger[-1]),
                        trade=self.ledger.trade_ledger[-1],
                    )
                    self.ledger.apply_pending_fills(prepared.decision_boundary_ts)
                execution_plan_hash = canonical_payload_hash(execution_evidence)
                fill_hash = canonical_payload_hash(
                    outcome.fill.as_dict() if outcome.fill is not None and hasattr(outcome.fill, "as_dict") else {}
                )
        else:
            outcome = None
            policy_position = getattr(policy_decision, "position_snapshot", None)
            if (
                bool(strategy_envelope.provenance.get("promotion_grade_policy_required"))
                and bool(getattr(policy_position, "has_executable_exposure", False))
            ):
                try:
                    planning = self.execution_planner.plan(
                        ExecutionPlanningRequest(
                            candle=candle,
                            event=event,
                            ledger=self.ledger,
                            strategy_name=self.strategy_plugin.name,
                            action=action,
                            decision_reason=risk_decision.reason_code,
                            sellable_qty=prepared.sellable_qty,
                            buy_fraction=self.buy_fraction,
                            promotion_grade_policy_required=True,
                            allow_execution_compatibility_fallback=bool(
                                strategy_envelope.provenance.get("allow_execution_compatibility_fallback")
                            ),
                            policy_drives_execution=True,
                            policy_decision=policy_decision,
                        )
                    )
                except ValueError as exc:
                    planning_error = str(exc)
                    if planning_error not in {
                        "research_submit_plan_missing",
                        "research_typed_submit_plan_missing",
                    }:
                        raise
                    self.warnings.append(planning_error)
                    execution_evidence = blocked_execution_evidence(planning_error)
                    planning_hash = canonical_payload_hash(execution_evidence)
                    self.trace_recorder.record_execution_planning(
                        input_hash=risk.risk_gate_hash,
                        execution_plan_hash=planning_hash,
                        reason_code=planning_error,
                    )
                else:
                    self.warnings.extend(planning.warnings)
                    execution_evidence = dict(planning.evidence)
                    planning_hash = canonical_payload_hash(execution_evidence)
                    self.trace_recorder.record_execution_planning(
                        input_hash=risk.risk_gate_hash,
                        execution_plan_hash=planning_hash,
                        reason_code=str(
                            execution_evidence.get("execution_plan_reason_code") or risk_decision.reason_code
                        ),
                    )
            else:
                execution_evidence = blocked_execution_evidence(risk_decision.reason_code)
            execution_plan_hash = canonical_payload_hash(execution_evidence)
            fill_hash = canonical_payload_hash({})
        self.trace_recorder.record_execution(
            input_hash=risk.risk_gate_hash if action not in {"BUY", "SELL"} else execution_plan_hash,
            execution_plan_hash=execution_plan_hash,
            fill_hash=fill_hash,
            reason_code=str(execution_evidence.get("execution_plan_reason_code") or risk_decision.reason_code),
        )
        return ExecutionStageResult(
            risk=risk,
            outcome=outcome,
            evidence=execution_evidence,
            execution_plan_hash=execution_plan_hash,
            fill_hash=fill_hash,
            mark_cash=mark_cash,
            mark_qty=mark_qty,
            decision_payload_qty=decision_payload_qty,
            decision_payload_sellable_qty=decision_payload_sellable_qty,
        )

    def _mark_ledger(
        self,
        prepared: TickPreparation,
        execution: ExecutionStageResult,
    ) -> LedgerStageResult:
        candle = prepared.tick.candle
        retain_equity = self.accumulator.retain_equity_point()
        self.ledger.mark_tick_equity(
            ts=prepared.mark_boundary_ts,
            mark_price=float(candle.close),
            cash=execution.mark_cash,
            qty=execution.mark_qty,
        )
        self.trace_recorder.record_ledger_and_equity(
            execution_plan_hash=execution.execution_plan_hash,
            ledger_snapshot=self.ledger.portfolio_snapshot(),
            mark_boundary_ts=prepared.mark_boundary_ts,
            mark_cash=execution.mark_cash,
            mark_qty=execution.mark_qty,
            mark_price=float(candle.close),
        )
        if not retain_equity and self.ledger.equity_curve:
            self.ledger.equity_curve.pop()
        self.accumulator.update_equity(
            retained=retain_equity,
            ts=prepared.mark_boundary_ts,
            asset_qty=execution.mark_qty,
        )
        return LedgerStageResult(
            execution=execution,
            mark_boundary_ts=prepared.mark_boundary_ts,
            mark_cash=execution.mark_cash,
            mark_qty=execution.mark_qty,
            retained_equity=retain_equity,
        )

    def _record_observability(
        self,
        prepared: TickPreparation,
        ledger: LedgerStageResult,
        event_number: int,
    ) -> ObservabilityStageResult:
        event = prepared.tick.event
        strategy = ledger.execution.risk.strategy
        strategy_envelope = strategy.envelope
        risk_decision = ledger.execution.risk.decision
        policy_decision = strategy_envelope.decision
        decision_payload = _build_decision_observability_payload(
            payload_builder=self.payload_builder,
            trace_recorder=self.trace_recorder,
            warnings=self.warnings,
            dataset=self.dataset,
            dataset_content_hash=self.dataset_content_hash,
            parameter_values=self.parameter_values,
            strategy_plugin=self.strategy_plugin,
            strategy_spec=self.strategy_spec,
            exit_policy=self.active_exit_policy,
            exit_policy_hash=self.active_exit_policy_hash,
            exit_policy_config_hash=self.active_exit_policy_config_hash,
            fee_rate=self.fee_rate,
            slippage_bps=self.slippage_bps,
            timing_policy=self.timing_policy,
            portfolio_policy=self.portfolio_policy,
            event=event,
            decision_boundary_ts=prepared.decision_boundary_ts,
            strategy_envelope=strategy_envelope,
            risk_decision=risk_decision,
            policy_position=strategy.position_snapshot,
            policy_decision=policy_decision,
            regime_snapshot=prepared.regime_snapshot,
            qty=ledger.execution.decision_payload_qty,
            sellable_qty=ledger.execution.decision_payload_sellable_qty,
            execution_evidence=ledger.execution.evidence,
            input_hash=ledger.execution.execution_plan_hash,
        )
        retain_decision = self.accumulator.retain_decision()
        if retain_decision:
            self.decisions.append(decision_payload)
        self.accumulator.update_decision(decision_payload, retained=retain_decision)
        _record_audit_decision(
            self.audit_recorder,
            self.run_context,
            self.warnings,
            self.trace_recorder,
            input_hash=canonical_payload_hash(decision_payload),
            decision_payload=decision_payload,
        )
        _record_audit_equity_mark(
            self.audit_recorder,
            self.run_context,
            self.warnings,
            self.trace_recorder,
            input_hash=canonical_payload_hash(
                {
                    "stage": "tick_equity",
                    "ts": ledger.mark_boundary_ts,
                    "cash": ledger.mark_cash,
                    "asset_qty": ledger.mark_qty,
                }
            ),
            ts=ledger.mark_boundary_ts,
            equity=ledger.mark_cash + ledger.mark_qty * float(prepared.tick.candle.close),
            cash=ledger.mark_cash,
            asset_qty=ledger.mark_qty,
        )
        _flush_stage_trace_observability(
            self.trace_recorder,
            self.warnings,
            count=6,
            metrics_collector=self.metrics_collector,
            experiment_recorder=self.experiment_recorder,
            event_number=event_number,
        )
        return ObservabilityStageResult(
            ledger=ledger,
            decision_payload=decision_payload,
            retained_decision=retain_decision,
        )


def run_stage_owned_decision_event_backtest(
    *,
    dataset: Any,
    strategy_name: str,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    decision_events: tuple[Any, ...],
    parameter_stability_score: float | None = None,
    execution_model: Any | None = None,
    execution_timing_policy: Any | None = None,
    portfolio_policy: Any | None = None,
    risk_policy: Any | None = None,
    context: Any | None = None,
    prepared_ticks: tuple[ReplayTick, ...] | None = None,
    prepared_ledger: PortfolioLedger | None = None,
    strategy_evaluator: Any | None = None,
    risk_gate: Any | None = None,
    execution_planner: Any | None = None,
    execution_simulator: Any | None = None,
    metrics_collector: Any | None = None,
    experiment_recorder: Any | None = None,
) -> Any:
    from .backtest_pipeline import BacktestPipelineState, DefaultMarketReplayClock
    from .strategy_registry import resolve_research_strategy_plugin

    strategy_plugin = resolve_research_strategy_plugin(strategy_name)
    strategy_spec = strategy_spec_for_name(strategy_name)
    active_exit_policy_materialization = exit_policy_materialization_from_parameters(strategy_name, parameter_values)
    active_exit_policy = dict(active_exit_policy_materialization.exit_policy)
    active_exit_policy_hash = exit_policy_hash(active_exit_policy)
    active_exit_policy_config_hash = str(active_exit_policy_materialization.exit_policy_config_hash)
    payload_builder = DecisionPayloadBuilder()
    audit_recorder = AuditTraceRecorder()
    trace_recorder = StageTraceRecorder()
    result_assembler = BacktestResultAssembler()
    candles = dataset.candles
    run_context = context or support.BacktestRunContext(report_detail="full")
    timing_policy = execution_timing_policy or ExecutionTimingPolicy()
    policy = portfolio_policy or legacy_research_portfolio_policy()
    effective_risk_policy = risk_policy or getattr(run_context, "risk_policy", None)
    if effective_risk_policy is None:
        from bithumb_bot.risk_contract import RiskPolicy

        effective_risk_policy = RiskPolicy(policy_status="disabled_explicit", source="research_default_disabled_explicit")
    model = execution_model or FixedBpsExecutionModel(fee_rate=fee_rate, slippage_bps=slippage_bps)
    starting_cash = float(policy.starting_cash_krw)
    ledger = prepared_ledger or PortfolioLedger.create(
        starting_cash=starting_cash,
        initial_position_qty=float(policy.initial_position_qty),
    )
    buy_fraction = float(policy.position_sizing.buy_fraction)
    accumulator = support.BacktestAccumulator(
        context=run_context,
        total_candles=len(candles),
        diagnostics_namespace=strategy_plugin.diagnostics_namespace,
    )
    if not candles:
        return result_assembler.empty_run(
            run_context=run_context,
            accumulator=accumulator,
            starting_cash=starting_cash,
            initial_position_qty=float(policy.initial_position_qty),
            parameter_stability_score=parameter_stability_score,
        )

    if prepared_ticks is None:
        prepared_ticks = DefaultMarketReplayClock().run(
            BacktestPipelineState(
                dataset=dataset,
                strategy_name=strategy_name,
                parameter_values=parameter_values,
                fee_rate=fee_rate,
                slippage_bps=slippage_bps,
                decision_events=decision_events,
                parameter_stability_score=parameter_stability_score,
                execution_model=execution_model,
                execution_timing_policy=timing_policy,
                portfolio_policy=policy,
                risk_policy=effective_risk_policy,
                context=run_context,
            )
        ).ticks

    dataset_content_hash = dataset.content_hash()
    decisions: list[dict[str, object]] = []
    warnings: list[str] = []
    regime_snapshots: list[dict[str, object]] = []
    regime_coverage_accumulator = support.RegimeCoverageAccumulator()

    first = candles[0]
    first_ts = candle_close_ts(first, interval=dataset.interval)
    retain_initial_equity = accumulator.retain_equity_point()
    if retain_initial_equity:
        ledger.equity_curve.append(
            EquityPoint(ts=first_ts, equity=starting_cash, cash=ledger.cash, asset_qty=ledger.qty)
        )
    accumulator.update_equity(retained=retain_initial_equity, ts=first_ts, asset_qty=ledger.qty)
    _record_audit_equity_mark(
        audit_recorder,
        run_context,
        warnings,
        trace_recorder,
        input_hash=canonical_payload_hash({"stage": "initial_equity", "ts": first_ts}),
        ts=first_ts,
        equity=starting_cash,
        cash=ledger.cash,
        asset_qty=ledger.qty,
    )

    event_processor = BacktestEventProcessor(
        dataset=dataset,
        strategy_name=strategy_name,
        parameter_values=parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
        timing_policy=timing_policy,
        execution_model=model,
        portfolio_policy=policy,
        risk_policy=effective_risk_policy,
        strategy_plugin=strategy_plugin,
        strategy_spec=strategy_spec,
        active_exit_policy=active_exit_policy,
        active_exit_policy_hash=active_exit_policy_hash,
        active_exit_policy_config_hash=active_exit_policy_config_hash,
        buy_fraction=buy_fraction,
        run_context=run_context,
        ledger=ledger,
        accumulator=accumulator,
        payload_builder=payload_builder,
        audit_recorder=audit_recorder,
        trace_recorder=trace_recorder,
        strategy_evaluator=strategy_evaluator,
        risk_gate=risk_gate,
        execution_planner=execution_planner,
        execution_simulator=execution_simulator,
        metrics_collector=metrics_collector,
        experiment_recorder=experiment_recorder,
        dataset_content_hash=dataset_content_hash,
        warnings=warnings,
        decisions=decisions,
        regime_snapshots=regime_snapshots,
        regime_coverage_accumulator=regime_coverage_accumulator,
    )
    for event_number, tick in enumerate(prepared_ticks, start=1):
        event_processor.process_tick(tick=tick, event_number=event_number)
        accumulator.maybe_emit_heartbeat(event_number)
        accumulator.check_limits(candles_processed=event_number, trades=ledger.trade_ledger)

    last = candles[-1]
    last_mark_ts = candle_close_ts(last, interval=dataset.interval)
    retain_final_equity = accumulator.retain_equity_point()
    finalization = ledger.finalize(
        last_mark_ts=last_mark_ts,
        last_price=float(last.close),
        retain_equity=retain_final_equity,
    )
    accumulator.update_equity(
        retained=finalization.equity_retained,
        ts=last_mark_ts,
        asset_qty=ledger.qty,
    )
    _record_audit_equity_mark(
        audit_recorder,
        run_context,
        warnings,
        trace_recorder,
        input_hash=canonical_payload_hash(
            {
                "stage": "final_equity",
                "ts": last_mark_ts,
                "cash": ledger.cash,
                "asset_qty": ledger.qty,
            }
        ),
        ts=last_mark_ts,
        equity=finalization.final_equity,
        cash=ledger.cash,
        asset_qty=ledger.qty,
    )

    return result_assembler.assemble(
        dataset=dataset,
        candles=tuple(candles),
        decision_events=decision_events,
        ledger=ledger,
        accumulator=accumulator,
        run_context=run_context,
        starting_cash=starting_cash,
        parameter_stability_score=parameter_stability_score,
        regime_snapshots=regime_snapshots,
        regime_coverage_accumulator=regime_coverage_accumulator,
        decisions=decisions,
        warnings=warnings,
        stage_trace_records=[trace.as_dict() for trace in trace_recorder.traces],
    )


def _build_decision_observability_payload(
    *,
    payload_builder: DecisionPayloadBuilder,
    trace_recorder: StageTraceRecorder,
    warnings: list[str],
    dataset: Any,
    dataset_content_hash: str,
    parameter_values: dict[str, Any],
    strategy_plugin: Any,
    strategy_spec: Any,
    exit_policy: dict[str, Any],
    exit_policy_hash: str,
    exit_policy_config_hash: str | None,
    fee_rate: float,
    slippage_bps: float,
    timing_policy: Any,
    portfolio_policy: Any,
    event: Any,
    decision_boundary_ts: int,
    strategy_envelope: Any,
    risk_decision: Any,
    policy_position: Any,
    policy_decision: Any | None,
    regime_snapshot: dict[str, object],
    qty: float,
    sellable_qty: float,
    execution_evidence: dict[str, object],
    input_hash: str,
) -> dict[str, object]:
    try:
        payload = payload_builder.build(
            dataset=dataset,
            dataset_content_hash=dataset_content_hash,
            parameter_values=parameter_values,
            strategy_plugin=strategy_plugin,
            strategy_spec=strategy_spec,
            exit_policy=exit_policy,
            exit_policy_hash=exit_policy_hash,
            exit_policy_config_hash=exit_policy_config_hash,
            fee_rate=fee_rate,
            slippage_bps=slippage_bps,
            timing_policy=timing_policy,
            portfolio_policy=portfolio_policy,
            event=event,
            decision_boundary_ts=decision_boundary_ts,
            strategy_envelope=strategy_envelope,
            risk_decision=risk_decision,
            policy_position=policy_position,
            policy_decision=policy_decision,
            regime_snapshot=regime_snapshot,
            qty=qty,
            sellable_qty=sellable_qty,
        )
        payload.update(dict(execution_evidence))
        if not bool(getattr(strategy_plugin, "is_promotion_grade", False)):
            payload["promotion_grade"] = False
            payload["promotion_extension_missing_reason"] = str(
                getattr(getattr(strategy_plugin, "runtime_capabilities", None), "fail_closed_reason", "")
            )
            payload["recommended_next_action"] = "promote_strategy_contract"
        return payload
    except Exception as exc:
        warnings.append("decision_payload_observability_failed")
        error_payload = {
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "strategy_name": str(strategy_plugin.name),
            "candle_ts": int(event.candle_ts),
            "decision_ts": int(decision_boundary_ts),
        }
        trace_recorder.record_observability_error(
            stage_id="decision_payload_observability",
            input_hash=input_hash,
            reason_code="decision_payload_observability_failed",
            payload=error_payload,
        )
        fallback = _minimal_decision_observability_payload(
            event=event,
            strategy_plugin=strategy_plugin,
            strategy_envelope=strategy_envelope,
            risk_decision=risk_decision,
            regime_snapshot=regime_snapshot,
            qty=qty,
            sellable_qty=sellable_qty,
            execution_evidence=execution_evidence,
        )
        fallback["decision_payload_observability_error"] = error_payload
        return fallback


def _minimal_decision_observability_payload(
    *,
    event: Any,
    strategy_plugin: Any,
    strategy_envelope: Any,
    risk_decision: Any,
    regime_snapshot: dict[str, object],
    qty: float,
    sellable_qty: float,
    execution_evidence: dict[str, object],
) -> dict[str, object]:
    raw_signal = str(strategy_envelope.provenance.get("raw_signal") or event.raw_signal or "HOLD").upper()
    entry_signal = str(strategy_envelope.provenance.get("entry_signal") or event.entry_signal or raw_signal).upper()
    exit_signal = str(strategy_envelope.provenance.get("exit_signal") or event.exit_signal or raw_signal).upper()
    payload: dict[str, object] = {
        "candle_ts": int(event.candle_ts),
        "decision_ts": int(event.decision_ts),
        "strategy_name": str(strategy_plugin.name),
        "strategy_diagnostics_namespace": str(strategy_plugin.diagnostics_namespace),
        "raw_signal": raw_signal,
        "entry_signal": entry_signal,
        "exit_signal": exit_signal,
        "final_signal": str(risk_decision.final_signal),
        "entry_reason": str(risk_decision.reason_code),
        "exit_rule": str(risk_decision.exit_rule or ""),
        "exit_reason": str(risk_decision.exit_reason or ""),
        "blocked_filters": tuple(strategy_envelope.provenance.get("blocked_filters") or ()),
        "feature_snapshot": dict(event.feature_snapshot),
        "current_market_regime_snapshot": dict(regime_snapshot),
        "regime_decision": "observability_unavailable",
        "regime_block_reason": "",
        "qty": float(qty),
        "sellable_qty": float(sellable_qty),
        "replay_fingerprint_hash": str(strategy_envelope.replay_fingerprint_hash or ""),
        "strategy_behavior_payload": {
            "strategy_name": str(strategy_plugin.name),
            "raw_signal": raw_signal,
            "final_signal": str(risk_decision.final_signal),
            "reason": str(risk_decision.reason_code),
            "feature_snapshot": dict(event.feature_snapshot),
        },
        "research_policy_unsupported": bool(strategy_envelope.unsupported_reason),
        "research_policy_unsupported_reason": str(strategy_envelope.unsupported_reason or ""),
        "research_policy_comparable": not bool(strategy_envelope.unsupported_reason),
    }
    payload.update(dict(execution_evidence))
    return payload


def _record_observability_error(
    *,
    trace_recorder: StageTraceRecorder,
    warnings: list[str],
    warning: str,
    stage_id: str,
    input_hash: str,
    exc: Exception,
) -> None:
    warnings.append(warning)
    trace_recorder.record_observability_error(
        stage_id=stage_id,
        input_hash=input_hash,
        reason_code=warning,
        payload={"error_type": type(exc).__name__, "error_message": str(exc)},
    )


def _record_audit_execution(
    audit_recorder: AuditTraceRecorder,
    run_context: Any,
    warnings: list[str],
    trace_recorder: StageTraceRecorder,
    *,
    input_hash: str,
    trade: dict[str, object],
) -> None:
    try:
        audit_recorder.record_execution(run_context, trade)
    except ArtifactBudgetExceeded:
        raise
    except Exception as exc:
        _record_observability_error(
            trace_recorder=trace_recorder,
            warnings=warnings,
            warning="audit_execution_observability_failed",
            stage_id="audit_execution_observability",
            input_hash=input_hash,
            exc=exc,
        )


def _record_audit_decision(
    audit_recorder: AuditTraceRecorder,
    run_context: Any,
    warnings: list[str],
    trace_recorder: StageTraceRecorder,
    *,
    input_hash: str,
    decision_payload: dict[str, object],
) -> None:
    try:
        audit_recorder.record_decision(run_context, decision_payload)
    except ArtifactBudgetExceeded:
        raise
    except Exception as exc:
        _record_observability_error(
            trace_recorder=trace_recorder,
            warnings=warnings,
            warning="audit_decision_observability_failed",
            stage_id="audit_decision_observability",
            input_hash=input_hash,
            exc=exc,
        )


def _record_audit_equity_mark(
    audit_recorder: AuditTraceRecorder,
    run_context: Any,
    warnings: list[str],
    trace_recorder: StageTraceRecorder,
    *,
    input_hash: str,
    ts: int,
    equity: float,
    cash: float,
    asset_qty: float,
) -> None:
    try:
        audit_recorder.record_equity_mark(
            run_context,
            ts=ts,
            equity=equity,
            cash=cash,
            asset_qty=asset_qty,
        )
    except ArtifactBudgetExceeded:
        raise
    except Exception as exc:
        _record_observability_error(
            trace_recorder=trace_recorder,
            warnings=warnings,
            warning="audit_equity_observability_failed",
            stage_id="audit_equity_observability",
            input_hash=input_hash,
            exc=exc,
        )


def _flush_stage_trace_observability(
    trace_recorder: StageTraceRecorder,
    warnings: list[str],
    *,
    count: int,
    metrics_collector: Any | None,
    experiment_recorder: Any | None,
    event_number: int,
) -> None:
    try:
        trace_recorder.flush_latest(
            count=count,
            metrics_collector=metrics_collector,
            experiment_recorder=experiment_recorder,
            event_number=event_number,
        )
    except Exception as exc:
        _record_observability_error(
            trace_recorder=trace_recorder,
            warnings=warnings,
            warning="stage_trace_observability_flush_failed",
            stage_id="stage_trace_observability",
            input_hash=canonical_payload_hash({"event_number": int(event_number)}),
            exc=exc,
        )
