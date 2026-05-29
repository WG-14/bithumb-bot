from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from bithumb_bot.canonical_decision import canonical_payload_hash
from bithumb_bot.market_regime import aggregate_regime_coverage, aggregate_regime_performance

from . import backtest_support as support
from .execution_timing import candle_close_ts
from .metrics_contract import EquityPoint, build_metrics_v2


@dataclass(frozen=True)
class BacktestResultAssembler:
    """Finalizes metrics and assembles the non-authoritative BacktestRun report."""

    def empty_run(
        self,
        *,
        run_context: Any,
        accumulator: support.BacktestAccumulator,
        starting_cash: float,
        initial_position_qty: float,
        parameter_stability_score: float | None,
    ) -> support.BacktestRun:
        warnings = ["not_enough_candles"]
        audit_trace_index = _complete_audit_trace_observability(
            run_context,
            warnings=warnings,
            status="completed",
        )
        return support.BacktestRun(
            metrics=support.empty_metrics(parameter_stability_score),
            metrics_v2=support.empty_metrics_v2(
                starting_cash=starting_cash,
                initial_position_qty=initial_position_qty,
            ),
            trades=(),
            candle_count=0,
            warnings=tuple(warnings),
            execution_event_summary=support.empty_execution_event_summary(),
            resource_usage=accumulator.resource_usage(candles_processed=0),
            strategy_diagnostics=accumulator.strategy_diagnostics(trades=[]),
            retained_detail_summary=support.retained_detail_summary(
                accumulator,
                retained_regime_snapshot_count=0,
            ),
            audit_trace_index=audit_trace_index,
        )

    def assemble(
        self,
        *,
        dataset: Any,
        candles: tuple[Any, ...],
        decision_events: tuple[Any, ...],
        ledger: Any,
        accumulator: support.BacktestAccumulator,
        run_context: Any,
        starting_cash: float,
        parameter_stability_score: float | None,
        regime_snapshots: list[dict[str, object]],
        regime_coverage_accumulator: support.RegimeCoverageAccumulator,
        decisions: list[dict[str, object]],
        warnings: list[str],
        stage_trace_records: list[dict[str, object]],
    ) -> support.BacktestRun:
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
        _trace_equity_mark_observability(
            run_context,
            warnings=warnings,
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
        audit_trace_index = _complete_audit_trace_observability(
            run_context,
            warnings=warnings,
            status="completed",
        )
        accumulator.trade_ledger_hash_material = [support.trade_hash_payload(trade) for trade in ledger.trade_ledger]
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
        resource_usage["stage_trace"] = stage_trace_records
        resource_usage["stage_trace_hash"] = canonical_payload_hash(stage_trace_records)
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


__all__ = ["BacktestResultAssembler"]


def _trace_equity_mark_observability(
    run_context: Any,
    *,
    warnings: list[str],
    ts: int,
    equity: float,
    cash: float,
    asset_qty: float,
) -> None:
    try:
        support.trace_equity_mark(
            run_context,
            ts=ts,
            equity=equity,
            cash=cash,
            asset_qty=asset_qty,
        )
    except Exception:
        warnings.append("audit_equity_observability_failed")


def _complete_audit_trace_observability(
    run_context: Any,
    *,
    warnings: list[str],
    status: str,
) -> dict[str, object] | None:
    try:
        return support.complete_audit_trace(run_context, status=status)
    except Exception:
        warnings.append("audit_trace_completion_observability_failed")
        return {"status": "audit_trace_completion_observability_failed"}
