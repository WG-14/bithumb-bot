from __future__ import annotations

from typing import Any

from . import backtest_support as support
from bithumb_bot.canonical_decision import canonical_payload_hash

from .audit_trace_recorder import AuditTraceRecorder
from .backtest_result_assembler import BacktestResultAssembler
from .backtest_stages import ReplayTick
from .decision_payload import DecisionPayloadBuilder
from .execution_simulator_stage import blocked_execution_evidence
from .execution_model import FixedBpsExecutionModel
from .execution_timing import candle_close_ts
from .experiment_manifest import ExecutionTimingPolicy, legacy_research_portfolio_policy
from .metrics_contract import EquityPoint
from .portfolio_ledger import PortfolioLedger
from .stage_trace_recorder import StageTraceRecorder
from .strategy_spec import exit_policy_from_parameters, exit_policy_hash, strategy_spec_for_name


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
    context: Any | None = None,
    prepared_ticks: tuple[ReplayTick, ...] | None = None,
    prepared_ledger: PortfolioLedger | None = None,
    strategy_evaluator: Any | None = None,
    risk_gate: Any | None = None,
    execution_simulator: Any | None = None,
    metrics_collector: Any | None = None,
    experiment_recorder: Any | None = None,
) -> Any:
    from .backtest_pipeline import BacktestPipelineState, DefaultMarketReplayClock
    from .strategy_registry import resolve_research_strategy_plugin

    strategy_plugin = resolve_research_strategy_plugin(strategy_name)
    strategy_spec = strategy_spec_for_name(strategy_name)
    active_exit_policy = exit_policy_from_parameters(strategy_name, parameter_values)
    active_exit_policy_hash = exit_policy_hash(active_exit_policy)
    payload_builder = DecisionPayloadBuilder()
    audit_recorder = AuditTraceRecorder()
    trace_recorder = StageTraceRecorder()
    result_assembler = BacktestResultAssembler()
    candles = dataset.candles
    run_context = context or support.BacktestRunContext(report_detail="full")
    timing_policy = execution_timing_policy or ExecutionTimingPolicy()
    policy = portfolio_policy or legacy_research_portfolio_policy()
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

    for event_number, tick in enumerate(prepared_ticks, start=1):
        event = tick.event
        candle = tick.candle
        mark_boundary_ts = candle_close_ts(candle, interval=dataset.interval)
        decision_boundary_ts = int(event.decision_ts)
        tick_state = ledger.begin_tick(
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
        regime_coverage_accumulator.update(regime_snapshot)
        if accumulator.retain_full_detail():
            regime_snapshots.append(regime_snapshot)

        policy_position = ledger.snapshot_for_policy(
            candle_ts=int(candle.ts),
            market_price=float(candle.close),
        )
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
            policy_position.as_dict() if hasattr(policy_position, "as_dict") else vars(policy_position)
        )
        strategy_envelope = strategy_evaluator.evaluate(
            tick,
            policy_position,
            {
                "dataset": dataset,
                "strategy_name": strategy_name,
                "parameter_values": parameter_values,
                "fee_rate": fee_rate,
                "slippage_bps": slippage_bps,
                "active_exit_policy": active_exit_policy,
                "buy_fraction": buy_fraction,
                "run_context": run_context,
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
        trace_recorder.record_strategy(
            replay_tick_hash=replay_tick_hash,
            position_snapshot_hash=position_snapshot_hash,
            strategy_decision_hash=strategy_decision_hash,
            compatibility_fallback=bool(strategy_envelope.compatibility_fallback),
            unsupported_reason=strategy_envelope.unsupported_reason,
            recommended_next_action=strategy_envelope.recommended_next_action,
        )
        policy_decision = strategy_envelope.decision
        risk_decision = risk_gate.evaluate(
            policy_decision,
            policy_position,
            {
                "candle_ts": int(candle.ts),
                "close": float(candle.close),
            },
            {
                "qty": ledger.qty,
                **ledger.portfolio_snapshot(tick_state),
            },
            {
                "strategy_plugin": strategy_plugin,
                "event": event,
                "active_exit_policy": active_exit_policy,
                "parameter_values": parameter_values,
                "fee_rate": fee_rate,
                "strategy_envelope": strategy_envelope,
            },
        )
        risk_gate_hash = risk_decision.evidence_hash
        trace_recorder.record_risk(
            input_hash=strategy_decision_hash,
            risk_gate_hash=risk_gate_hash,
            reason_code=risk_decision.reason_code,
        )
        action = risk_decision.final_signal
        execution_evidence: dict[str, object]
        decision_payload_qty = float(ledger.qty)
        decision_payload_sellable_qty = float(sellable_qty)
        if action in {"BUY", "SELL"}:
            outcome = execution_simulator.execute(
                dataset=dataset,
                candle=candle,
                candle_index=int(tick.candle_index),
                event=event,
                ledger=ledger,
                timing_policy=timing_policy,
                execution_model=model,
                fee_rate=fee_rate,
                strategy_name=strategy_plugin.name,
                action=action,
                decision_reason=risk_decision.reason_code,
                regime_snapshot=regime_snapshot,
                decision_hash=str(strategy_envelope.replay_fingerprint_hash or strategy_decision_hash),
                sellable_qty=sellable_qty,
                buy_fraction=buy_fraction,
                promotion_grade_policy_required=bool(
                    strategy_envelope.provenance.get("promotion_grade_policy_required")
                ),
                allow_execution_compatibility_fallback=bool(
                    strategy_envelope.provenance.get("allow_execution_compatibility_fallback")
                ),
                policy_drives_execution=True,
                policy_decision=policy_decision,
                exit_rule=risk_decision.exit_rule,
                exit_reason=risk_decision.exit_reason,
            )
            execution_evidence = dict(outcome.evidence)
            warnings.extend(outcome.warnings)
            application = ledger.apply_execution_outcome(
                outcome,
                mark_boundary_ts=mark_boundary_ts,
                mark_cash=mark_cash,
                mark_qty=mark_qty,
            )
            mark_cash = application.mark_cash
            mark_qty = application.mark_qty
            if application.trade_recorded:
                _record_audit_execution(
                    audit_recorder,
                    run_context,
                    warnings,
                    trace_recorder,
                    input_hash=canonical_payload_hash(ledger.trade_ledger[-1]),
                    trade=ledger.trade_ledger[-1],
                )
                ledger.apply_pending_fills(decision_boundary_ts)
            execution_plan_hash = canonical_payload_hash(execution_evidence)
            fill_hash = canonical_payload_hash(
                outcome.fill.as_dict() if outcome.fill is not None and hasattr(outcome.fill, "as_dict") else {}
            )
        else:
            execution_evidence = blocked_execution_evidence(risk_decision.reason_code)
            execution_plan_hash = canonical_payload_hash(execution_evidence)
            fill_hash = canonical_payload_hash({})
        trace_recorder.record_execution(
            input_hash=risk_gate_hash,
            execution_plan_hash=execution_plan_hash,
            fill_hash=fill_hash,
            reason_code=str(execution_evidence.get("execution_plan_reason_code") or risk_decision.reason_code),
        )

        retain_equity = accumulator.retain_equity_point()
        ledger.mark_tick_equity(
            ts=mark_boundary_ts,
            mark_price=float(candle.close),
            cash=mark_cash,
            qty=mark_qty,
        )
        trace_recorder.record_ledger_and_equity(
            execution_plan_hash=execution_plan_hash,
            ledger_snapshot=ledger.portfolio_snapshot(),
            mark_boundary_ts=mark_boundary_ts,
            mark_cash=mark_cash,
            mark_qty=mark_qty,
            mark_price=float(candle.close),
        )
        if not retain_equity and ledger.equity_curve:
            ledger.equity_curve.pop()
        accumulator.update_equity(retained=retain_equity, ts=mark_boundary_ts, asset_qty=mark_qty)
        decision_payload = _build_decision_observability_payload(
            payload_builder=payload_builder,
            trace_recorder=trace_recorder,
            warnings=warnings,
            dataset=dataset,
            dataset_content_hash=dataset_content_hash,
            parameter_values=parameter_values,
            strategy_plugin=strategy_plugin,
            strategy_spec=strategy_spec,
            exit_policy=active_exit_policy,
            exit_policy_hash=active_exit_policy_hash,
            fee_rate=fee_rate,
            slippage_bps=slippage_bps,
            timing_policy=timing_policy,
            portfolio_policy=policy,
            event=event,
            decision_boundary_ts=decision_boundary_ts,
            strategy_envelope=strategy_envelope,
            risk_decision=risk_decision,
            policy_position=policy_position,
            policy_decision=policy_decision,
            regime_snapshot=regime_snapshot,
            qty=decision_payload_qty,
            sellable_qty=decision_payload_sellable_qty,
            execution_evidence=execution_evidence,
            input_hash=execution_plan_hash,
        )
        retain_decision = accumulator.retain_decision()
        if retain_decision:
            decisions.append(decision_payload)
        accumulator.update_decision(decision_payload, retained=retain_decision)
        _record_audit_decision(
            audit_recorder,
            run_context,
            warnings,
            trace_recorder,
            input_hash=canonical_payload_hash(decision_payload),
            decision_payload=decision_payload,
        )
        _record_audit_equity_mark(
            audit_recorder,
            run_context,
            warnings,
            trace_recorder,
            input_hash=canonical_payload_hash(
                {
                    "stage": "tick_equity",
                    "ts": mark_boundary_ts,
                    "cash": mark_cash,
                    "asset_qty": mark_qty,
                }
            ),
            ts=mark_boundary_ts,
            equity=mark_cash + mark_qty * float(candle.close),
            cash=mark_cash,
            asset_qty=mark_qty,
        )
        _flush_stage_trace_observability(
            trace_recorder,
            warnings,
            count=5,
            metrics_collector=metrics_collector,
            experiment_recorder=experiment_recorder,
            event_number=event_number,
        )
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
