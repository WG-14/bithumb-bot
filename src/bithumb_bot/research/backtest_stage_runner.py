from __future__ import annotations

from dataclasses import replace
from typing import Any

from bithumb_bot.market_regime import aggregate_regime_coverage, aggregate_regime_performance

from . import backtest_support as support
from bithumb_bot.canonical_decision import canonical_payload_hash

from .backtest_stages import ReplayTick, StageTrace
from .execution_simulator_stage import blocked_execution_evidence
from .execution_model import FixedBpsExecutionModel
from .execution_timing import candle_close_ts
from .experiment_manifest import ExecutionTimingPolicy, legacy_research_portfolio_policy
from .metrics_contract import EquityPoint, build_metrics_v2
from .portfolio_ledger import PortfolioLedger
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
        audit_trace_index = support.complete_audit_trace(run_context, status="completed")
        return support.BacktestRun(
            metrics=support.empty_metrics(parameter_stability_score),
            metrics_v2=support.empty_metrics_v2(
                starting_cash=starting_cash,
                initial_position_qty=float(policy.initial_position_qty),
            ),
            trades=(),
            candle_count=0,
            warnings=("not_enough_candles",),
            execution_event_summary=support.empty_execution_event_summary(),
            resource_usage=accumulator.resource_usage(candles_processed=0),
            strategy_diagnostics=accumulator.strategy_diagnostics(trades=[]),
            retained_detail_summary=support.retained_detail_summary(
                accumulator,
                retained_regime_snapshot_count=0,
            ),
            audit_trace_index=audit_trace_index,
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
    stage_traces: list[StageTrace] = []
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
    support.trace_equity_mark(
        run_context,
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
        stage_traces.append(
            StageTrace(
                stage_id="strategy",
                input_hash=canonical_payload_hash(
                    {"replay_tick_hash": replay_tick_hash, "position_snapshot_hash": position_snapshot_hash}
                ),
                output_hash=strategy_decision_hash,
                reason_code=str(strategy_envelope.unsupported_reason or "OK"),
                payload={
                    "replay_tick_hash": replay_tick_hash,
                    "position_snapshot_hash": position_snapshot_hash,
                    "strategy_decision_hash": strategy_decision_hash,
                    "compatibility_fallback": bool(strategy_envelope.compatibility_fallback),
                    "recommended_next_action": strategy_envelope.recommended_next_action,
                },
            )
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
        stage_traces.append(
            StageTrace(
                stage_id="risk",
                input_hash=strategy_decision_hash,
                output_hash=risk_gate_hash,
                reason_code=risk_decision.reason_code,
                payload={"risk_gate_hash": risk_gate_hash},
            )
        )
        action = risk_decision.final_signal
        raw_signal = str(strategy_envelope.provenance.get("raw_signal") or "HOLD").upper()
        raw_reason = str(strategy_envelope.provenance.get("raw_reason") or event.reason)
        raw_filter_would_block = bool(strategy_envelope.provenance.get("raw_filter_would_block"))
        entry_signal = str(strategy_envelope.provenance.get("entry_signal") or raw_signal).upper()
        exit_signal = str(strategy_envelope.provenance.get("exit_signal") or raw_signal).upper()
        blocked_filters = list(strategy_envelope.provenance.get("blocked_filters") or ())
        entry_decision = strategy_envelope.provenance.get("entry_decision")
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
        if policy_decision is not None:
            protective_exit_overrode_entry = bool(policy_decision.protective_exit_overrode_entry)
            entry_blocked = bool(policy_decision.entry_blocked)
            exit_filter_suppression_prevented = bool(policy_decision.exit_filter_suppression_prevented)
        elif strategy_envelope.unsupported_reason:
            protective_exit_overrode_entry = False
            entry_blocked = False
            exit_filter_suppression_prevented = False
        else:
            protective_exit_overrode_entry = bool(
                raw_signal == "BUY"
                and action == "SELL"
                and risk_decision.exit_rule in {"stop_loss", "max_holding_time"}
            )
            entry_blocked = bool(raw_signal == "BUY" and action == "HOLD" and raw_filter_would_block)
            exit_filter_suppression_prevented = bool(
                raw_signal == "SELL"
                and raw_filter_would_block
                and sellable_qty > 1e-12
                and bool(risk_decision.exit_evaluations)
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
            blocked=bool(risk_decision.block or (raw_signal in {"BUY", "SELL"} and action == "HOLD")),
            raw_filter_would_block=raw_filter_would_block,
            entry_blocked=entry_blocked,
            protective_exit_overrode_entry=protective_exit_overrode_entry,
            exit_filter_suppression_prevented=exit_filter_suppression_prevented,
            blocked_filters=blocked_filters,
            feature_snapshot=dict(event.feature_snapshot),
            regime_snapshot=regime_snapshot,
            entry_reason=risk_decision.reason_code,
            market_regime_decision=market_regime_decision,
            market_regime_blocked=market_regime_blocked,
            candidate_regime_blocked=candidate_regime_blocked,
            qty=ledger.qty,
            sellable_qty=sellable_qty,
            exit_rule=risk_decision.exit_rule,
            exit_reason=risk_decision.exit_reason,
            exit_evaluations=[dict(item) for item in risk_decision.exit_evaluations],
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
                    "reason": risk_decision.reason_code,
                    "feature_snapshot": dict(event.feature_snapshot),
                    "strategy_diagnostics": dict(event.strategy_diagnostics),
                },
                "execution_intent": action.lower() if action in {"BUY", "SELL"} else "none",
                "order_intent": dict(event.order_intent) if event.order_intent is not None else None,
                "exit_intent": dict(event.exit_intent) if event.exit_intent is not None else None,
                "research_policy_position_terminal_state": policy_position.terminal_state,
                "research_policy_recomputed_with_simulated_position": policy_decision is not None,
                "research_policy_unsupported": bool(strategy_envelope.unsupported_reason),
                "research_policy_unsupported_reason": strategy_envelope.unsupported_reason,
                "research_policy_comparable": not bool(strategy_envelope.unsupported_reason),
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
                decision_hash=str(decision_payload.get("replay_fingerprint_hash") or ""),
                sellable_qty=sellable_qty,
                buy_fraction=buy_fraction,
                promotion_grade_policy_required=bool(
                    strategy_envelope.provenance.get("promotion_grade_policy_required")
                ),
                allow_execution_compatibility_fallback=bool(
                    policy_decision is None
                    and not strategy_envelope.unsupported_reason
                    and (
                        strategy_plugin.research_policy_decision_builder is None
                        or bool(strategy_envelope.provenance.get("allows_legacy_event_first_exit_policy"))
                    )
                ),
                policy_drives_execution=True,
                policy_decision=policy_decision,
                exit_rule=risk_decision.exit_rule,
                exit_reason=risk_decision.exit_reason,
            )
            decision_payload.update(dict(outcome.evidence))
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
                support.trace_execution(run_context, ledger.trade_ledger[-1])
                ledger.apply_pending_fills(decision_boundary_ts)
            execution_plan_hash = canonical_payload_hash(dict(outcome.evidence))
            fill_hash = canonical_payload_hash(
                outcome.fill.as_dict() if outcome.fill is not None and hasattr(outcome.fill, "as_dict") else {}
            )
        else:
            blocked_evidence = blocked_execution_evidence(risk_decision.reason_code)
            decision_payload.update(blocked_evidence)
            execution_plan_hash = canonical_payload_hash(blocked_evidence)
            fill_hash = canonical_payload_hash({})
        stage_traces.append(
            StageTrace(
                stage_id="execution",
                input_hash=risk_gate_hash,
                output_hash=execution_plan_hash,
                reason_code=str(decision_payload.get("execution_plan_reason_code") or risk_decision.reason_code),
                payload={"execution_plan_hash": execution_plan_hash, "fill_hash": fill_hash},
            )
        )
        retain_decision = accumulator.retain_decision()
        if retain_decision:
            decisions.append(decision_payload)
        accumulator.update_decision(decision_payload, retained=retain_decision)
        support.trace_decision(run_context, decision_payload)

        retain_equity = accumulator.retain_equity_point()
        ledger.mark_tick_equity(
            ts=mark_boundary_ts,
            mark_price=float(candle.close),
            cash=mark_cash,
            qty=mark_qty,
        )
        ledger_hash = canonical_payload_hash(ledger.portfolio_snapshot())
        equity_hash = canonical_payload_hash(
            {
                "ts": int(mark_boundary_ts),
                "cash": round(float(mark_cash), 12),
                "asset_qty": round(float(mark_qty), 12),
                "mark_price": round(float(candle.close), 12),
            }
        )
        stage_traces.append(
            StageTrace(
                stage_id="ledger",
                input_hash=execution_plan_hash,
                output_hash=ledger_hash,
                reason_code="OK",
                payload={"ledger_hash": ledger_hash},
            )
        )
        stage_traces.append(
            StageTrace(
                stage_id="equity",
                input_hash=ledger_hash,
                output_hash=equity_hash,
                reason_code="OK",
                payload={"equity_hash": equity_hash},
            )
        )
        if not retain_equity and ledger.equity_curve:
            ledger.equity_curve.pop()
        accumulator.update_equity(retained=retain_equity, ts=mark_boundary_ts, asset_qty=mark_qty)
        support.trace_equity_mark(
            run_context,
            ts=mark_boundary_ts,
            equity=mark_cash + mark_qty * float(candle.close),
            cash=mark_cash,
            asset_qty=mark_qty,
        )
        if metrics_collector is not None:
            metrics_collector.record(
                "stage_trace",
                {
                    "event_number": event_number,
                    "stage_traces": [trace.as_dict() for trace in stage_traces[-5:]],
                },
            )
        if experiment_recorder is not None:
            for trace in stage_traces[-5:]:
                experiment_recorder.record_stage(**trace.as_dict())
        accumulator.maybe_emit_heartbeat(event_number)
        accumulator.check_limits(candles_processed=event_number, trades=ledger.trade_ledger)

    last = candles[-1]
    last_mark_ts = candle_close_ts(last, interval=dataset.interval)
    ledger.apply_pending_fills(last_mark_ts)
    support.mark_pending_fills_at_end(
        pending_fills=ledger.pending_fills,
        trades=ledger.trade_ledger,
        final_mark_ts=last_mark_ts,
    )
    final_equity = ledger.cash + ledger.qty * float(last.close)
    retain_final_equity = accumulator.retain_equity_point()
    if retain_final_equity:
        ledger.equity_curve.append(
            EquityPoint(ts=last_mark_ts, equity=final_equity, cash=ledger.cash, asset_qty=ledger.qty)
        )
    accumulator.update_equity(retained=retain_final_equity, ts=last_mark_ts, asset_qty=ledger.qty)
    support.trace_equity_mark(
        run_context,
        ts=last_mark_ts,
        equity=final_equity,
        cash=ledger.cash,
        asset_qty=ledger.qty,
    )
    return_pct = ((final_equity / starting_cash) - 1.0) * 100.0 if starting_cash > 0.0 else 0.0
    metrics = support.metrics(
        return_pct=return_pct,
        max_drawdown_pct=ledger.max_drawdown * 100.0,
        closed_pnls=ledger.closed_pnls,
        fee_total=ledger.fee_total,
        slippage_total=ledger.slippage_total,
        parameter_stability_score=parameter_stability_score,
    )
    (
        position_intervals,
        closed_trade_records,
        execution_records,
        derived_open_cost_basis,
    ) = support.metrics_v2_ledgers_from_trades(trades=ledger.trade_ledger)
    coverage = (
        aggregate_regime_coverage(snapshots=regime_snapshots, trades=ledger.trade_ledger)
        if accumulator.retain_full_detail()
        else regime_coverage_accumulator.coverage(trades=ledger.trade_ledger)
    )
    performance = aggregate_regime_performance(
        trades=ledger.trade_ledger,
        coverage=coverage,
        start_cash=starting_cash,
    )
    metrics_v2 = build_metrics_v2(
        starting_cash=starting_cash,
        final_cash=ledger.cash,
        final_asset_qty=ledger.qty,
        final_mark_price=last.close,
        final_open_cost_basis=ledger.entry_cost_basis if ledger.qty > 0.0 else derived_open_cost_basis,
        equity_curve=tuple(ledger.equity_curve),
        position_intervals=position_intervals,
        closed_trades=closed_trade_records,
        execution_records=execution_records,
        **(
            {}
            if accumulator.retain_full_detail()
            else accumulator.metrics_summary_inputs(max_drawdown_pct=ledger.max_drawdown * 100.0)
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
    accumulator.trade_ledger_hash_material = [
        support.trade_hash_payload(trade) for trade in ledger.trade_ledger
    ]
    accumulator.equity_curve_hash_material = [
        {
            "ts": int(point.ts),
            "equity": round(float(point.equity), 12),
            "cash": round(float(point.cash), 12),
            "asset_qty": round(float(point.asset_qty), 12),
        }
        for point in ledger.equity_curve
    ]
    strategy_diagnostics = accumulator.strategy_diagnostics(trades=ledger.trade_ledger)
    resource_usage = accumulator.resource_usage(candles_processed=len(decision_events))
    resource_usage["strategy_diagnostics"] = strategy_diagnostics
    resource_usage["stage_trace"] = [trace.as_dict() for trace in stage_traces]
    resource_usage["stage_trace_hash"] = canonical_payload_hash(resource_usage["stage_trace"])
    return support.BacktestRun(
        metrics=metrics,
        metrics_v2=metrics_v2,
        trades=tuple(ledger.trade_ledger),
        candle_count=len(candles),
        warnings=tuple(warnings),
        regime_performance=performance,
        regime_coverage=coverage,
        execution_event_summary=support.execution_event_summary(ledger.trade_ledger),
        decisions=tuple(decisions),
        equity_curve=tuple(ledger.equity_curve),
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
