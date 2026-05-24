from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from bithumb_bot.market_regime import aggregate_regime_coverage, aggregate_regime_performance
from bithumb_bot.strategy.exit_rules import merge_exit_rules

from . import backtest_engine as _engine
from .decision_event import ResearchDecisionEvent
from .execution_model import ExecutionRequest, FixedBpsExecutionModel
from .execution_timing import build_signal_event, candle_close_ts, resolve_execution_reference
from .experiment_manifest import ExecutionTimingPolicy, legacy_research_portfolio_policy
from .metrics_contract import EquityPoint, build_metrics_v2
from .strategy_spec import exit_policy_from_parameters, exit_policy_hash, strategy_spec_for_name

if TYPE_CHECKING:
    from .backtest_engine import BacktestRun, BacktestRunContext
    from .dataset_snapshot import DatasetSnapshot
    from .execution_model import ExecutionModel
    from .experiment_manifest import ExecutionTimingPolicy, PortfolioPolicy


# BacktestEngine still owns the surrounding research data structures and helper
# graph. The decision-event loop is implemented here and calls those helpers as
# a compatibility dependency while callers bind to this public kernel module.
BacktestRun = _engine.BacktestRun
BacktestRunContext = _engine.BacktestRunContext
_BacktestAccumulator = _engine._BacktestAccumulator
_PendingFill = _engine._PendingFill
_RegimeCoverageAccumulator = _engine._RegimeCoverageAccumulator
_ResearchPositionContext = _engine._ResearchPositionContext
_apply_pending_fills = _engine._apply_pending_fills
_closed_trade_diagnostics = _engine._closed_trade_diagnostics
_complete_audit_trace = _engine._complete_audit_trace
_create_exit_rules = _engine._create_exit_rules
_depth_request_fields = _engine._depth_request_fields
_empty_metrics = _engine._empty_metrics
_empty_metrics_v2 = _engine._empty_metrics_v2
_execution_reference_warnings = _engine._execution_reference_warnings
_failed_fill = _engine._failed_fill
_fill_applies_to_mark = _engine._fill_applies_to_mark
_fill_effective_ts = _engine._fill_effective_ts
_mark_pending_fills_at_end = _engine._mark_pending_fills_at_end
_metrics = _engine._metrics
_metrics_v2_ledgers_from_trades = _engine._metrics_v2_ledgers_from_trades
_model_latency_ms = _engine._model_latency_ms
_pending_trade_from_fill = _engine._pending_trade_from_fill
_record_equity_mark = _engine._record_equity_mark
_research_decision_payload = _engine._research_decision_payload
_retained_detail_summary = _engine._retained_detail_summary
_timing_request_fields = _engine._timing_request_fields
_trace_decision = _engine._trace_decision
_trace_equity_mark = _engine._trace_equity_mark
_trace_execution = _engine._trace_execution
_trade_from_fill = _engine._trade_from_fill
_trade_hash_payload = _engine._trade_hash_payload
empty_execution_event_summary = _engine.empty_execution_event_summary
execution_event_summary = _engine.execution_event_summary

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
    accumulator = _BacktestAccumulator(
        context=run_context,
        total_candles=len(candles),
        diagnostics_namespace=strategy_plugin.diagnostics_namespace,
    )
    if not candles:
        audit_trace_index = _complete_audit_trace(run_context, status="completed")
        return BacktestRun(
            metrics=_empty_metrics(parameter_stability_score),
            metrics_v2=_empty_metrics_v2(starting_cash=starting_cash, initial_position_qty=qty),
            trades=(),
            candle_count=0,
            warnings=("not_enough_candles",),
            execution_event_summary=empty_execution_event_summary(),
            resource_usage=accumulator.resource_usage(candles_processed=0),
            strategy_diagnostics=accumulator.strategy_diagnostics(trades=[]),
            retained_detail_summary=_retained_detail_summary(accumulator, retained_regime_snapshot_count=0),
            audit_trace_index=audit_trace_index,
        )

    dataset_content_hash = dataset.content_hash()
    candle_index_by_ts = {int(candle.ts): index for index, candle in enumerate(candles)}
    trades: list[dict[str, object]] = []
    decisions: list[dict[str, object]] = []
    equity_curve: list[EquityPoint] = []
    pending_fills: list[_PendingFill] = []
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
    regime_coverage_accumulator = _RegimeCoverageAccumulator()

    first = candles[0]
    first_ts = candle_close_ts(first, interval=dataset.interval)
    retain_initial_equity = accumulator.retain_equity_point()
    if retain_initial_equity:
        equity_curve.append(EquityPoint(ts=first_ts, equity=starting_cash, cash=cash, asset_qty=qty))
    accumulator.update_equity(retained=retain_initial_equity, ts=first_ts, asset_qty=qty)
    _trace_equity_mark(run_context, ts=first_ts, equity=starting_cash, cash=cash, asset_qty=qty)

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
        ) = _apply_pending_fills(
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
        ) = _apply_pending_fills(
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
        requested_action = str(event.final_signal or "HOLD").upper()
        action = requested_action
        blocked = False
        block_reason = event.reason
        exit_evaluations: list[dict[str, object]] = []
        exit_rule = str((event.exit_intent or {}).get("exit_rule") or "") if event.exit_intent else ""
        exit_reason = str((event.exit_intent or {}).get("exit_reason") or "") if event.exit_intent else ""
        evaluates_exit_policy = bool(
            isinstance(event.exit_intent, dict)
            and str(event.exit_intent.get("mode") or "") == "evaluate_exit_policy"
        )
        if evaluates_exit_policy:
            action = "BUY" if requested_action == "BUY" else "HOLD"
            if sellable_qty > 1e-12:
                position = _ResearchPositionContext(
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
                common_exit_rules = _create_exit_rules(
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
        decision_payload = _research_decision_payload(
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
            exit_signal=event.exit_signal or event.raw_signal,
            final_signal=action,
            raw_reason=raw_reason,
            blocked=bool(blocked or (raw_signal in {"BUY", "SELL"} and action == "HOLD")),
            raw_filter_would_block=raw_filter_would_block,
            entry_blocked=entry_blocked,
            protective_exit_overrode_entry=protective_exit_overrode_entry,
            exit_filter_suppression_prevented=exit_filter_suppression_prevented,
            blocked_filters=list(event.blocked_filters),
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
                "raw_reason": event.reason,
                "feature_snapshot": dict(event.feature_snapshot),
                "strategy_diagnostics_namespace": strategy_plugin.diagnostics_namespace,
                "strategy_diagnostics": dict(event.strategy_diagnostics),
                "strategy_behavior_payload": {
                    "strategy_name": event.strategy_name,
                    "strategy_version": event.strategy_version,
                    "raw_signal": event.raw_signal,
                    "final_signal": action,
                    "reason": event.reason,
                    "feature_snapshot": dict(event.feature_snapshot),
                    "strategy_diagnostics": dict(event.strategy_diagnostics),
                },
                "execution_intent": action.lower() if action in {"BUY", "SELL"} else "none",
                "order_intent": dict(event.order_intent) if event.order_intent is not None else None,
                "exit_intent": dict(event.exit_intent) if event.exit_intent is not None else None,
            }
        )
        retain_decision = accumulator.retain_decision()
        if retain_decision:
            decisions.append(decision_payload)
        accumulator.update_decision(decision_payload, retained=retain_decision)
        _trace_decision(run_context, decision_payload)

        if action in {"BUY", "SELL"}:
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
                model_latency_ms=_model_latency_ms(model),
            )
            requested_notional = cash * buy_fraction if side == "BUY" else None
            requested_qty = sellable_qty if side == "SELL" else None
            if reference.fill_reference_price is None:
                fill = _failed_fill(
                    model=model,
                    signal=signal,
                    reference=reference,
                    timing_policy=timing_policy,
                    side=side,
                    fee_rate=fee_rate,
                    requested_qty=requested_qty,
                    requested_notional=requested_notional,
                )
            else:
                fill = model.simulate(
                    ExecutionRequest(
                        signal_ts=signal.signal_candle_start_ts,
                        decision_ts=signal.decision_ts,
                        side=side,
                        reference_price=float(reference.fill_reference_price),
                        requested_qty=requested_qty,
                        requested_notional=requested_notional,
                        fee_rate=fee_rate,
                        **_timing_request_fields(signal, reference, timing_policy),
                        **_depth_request_fields(
                            dataset=dataset,
                            reference=reference,
                            model=model,
                            timing_policy=timing_policy,
                        ),
                    )
                )
            warnings.extend(_execution_reference_warnings(fill))
            if fill.fill_status == "failed" or fill.avg_fill_price is None or fill.filled_qty <= 0.0:
                trades.append(_trade_from_fill(fill, cash=cash, asset_qty=qty, pnl=None))
                _trace_execution(run_context, trades[-1])
            elif side == "BUY":
                exec_price = float(fill.avg_fill_price)
                fee = float(fill.fee)
                received_qty = float(fill.filled_qty)
                actual_spend = (exec_price * received_qty) + fee
                buy_slippage = max(0.0, (exec_price - float(fill.reference_price)) * received_qty)
                pending = _PendingFill(
                    fill=fill,
                    trade_index=len(trades),
                    side="BUY",
                    effective_ts=_fill_effective_ts(fill),
                    qty=received_qty,
                    fee=fee,
                    slippage=buy_slippage,
                    cash_delta=-actual_spend,
                    entry_regime_snapshot=regime_snapshot,
                )
                trades.append(_pending_trade_from_fill(fill, cash=cash, asset_qty=qty))
                trades[-1]["entry_decision_hash"] = decision_payload.get("replay_fingerprint_hash")
                _trace_execution(run_context, trades[-1])
                if _fill_applies_to_mark(fill=pending.fill, effective_ts=pending.effective_ts, mark_boundary_ts=mark_boundary_ts):
                    mark_cash += pending.cash_delta
                    mark_qty += pending.qty
                pending_fills.append(pending)
            else:
                exec_price = float(fill.avg_fill_price)
                sell_qty = float(fill.filled_qty)
                gross = sell_qty * exec_price
                fee = float(fill.fee)
                sell_slippage = max(0.0, (float(fill.reference_price) - exec_price) * sell_qty)
                pending = _PendingFill(
                    fill=fill,
                    trade_index=len(trades),
                    side="SELL",
                    effective_ts=_fill_effective_ts(fill),
                    qty=sell_qty,
                    fee=fee,
                    slippage=sell_slippage,
                    cash_delta=gross - fee,
                    entry_regime_snapshot=entry_regime_snapshot,
                    exit_regime_snapshot=regime_snapshot,
                )
                trades.append(_pending_trade_from_fill(fill, cash=cash, asset_qty=qty))
                trades[-1].update(
                    _closed_trade_diagnostics(
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
                _trace_execution(run_context, trades[-1])
                if _fill_applies_to_mark(fill=pending.fill, effective_ts=pending.effective_ts, mark_boundary_ts=mark_boundary_ts):
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
            ) = _apply_pending_fills(
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
        peak, max_drawdown = _record_equity_mark(
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
        _trace_equity_mark(
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
    ) = _apply_pending_fills(
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

    _mark_pending_fills_at_end(pending_fills=pending_fills, trades=trades, final_mark_ts=last_mark_ts)
    final_equity = cash + qty * float(last.close)
    retain_final_equity = accumulator.retain_equity_point()
    if retain_final_equity:
        equity_curve.append(EquityPoint(ts=last_mark_ts, equity=final_equity, cash=cash, asset_qty=qty))
    accumulator.update_equity(retained=retain_final_equity, ts=last_mark_ts, asset_qty=qty)
    _trace_equity_mark(run_context, ts=last_mark_ts, equity=final_equity, cash=cash, asset_qty=qty)
    return_pct = ((final_equity / starting_cash) - 1.0) * 100.0 if starting_cash > 0.0 else 0.0
    metrics = _metrics(
        return_pct=return_pct,
        max_drawdown_pct=max_drawdown * 100.0,
        closed_pnls=closed_pnls,
        fee_total=fee_total,
        slippage_total=slippage_total,
        parameter_stability_score=parameter_stability_score,
    )
    position_intervals, closed_trade_records, execution_records, derived_open_cost_basis = _metrics_v2_ledgers_from_trades(
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
    audit_trace_index = _complete_audit_trace(run_context, status="completed")
    accumulator.trade_ledger_hash_material = [_trade_hash_payload(trade) for trade in trades]
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
        retained_detail_summary=_retained_detail_summary(
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
