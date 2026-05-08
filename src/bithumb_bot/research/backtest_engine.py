from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bithumb_bot.market_regime import (
    RegimeCoverageRow,
    RegimePerformanceRow,
    aggregate_regime_coverage,
    aggregate_regime_performance,
    classify_market_regime,
)
from bithumb_bot.market_regime.thresholds import MarketRegimeThresholds
from bithumb_bot.canonical_decision import canonical_flat_position_state_hash, canonical_payload_hash

from .dataset_snapshot import DatasetSnapshot
from .execution_model import ExecutionFill, ExecutionModel, ExecutionRequest, FixedBpsExecutionModel, model_params_hash
from .execution_timing import (
    ExecutionReferenceEvent,
    SignalEvent,
    build_signal_event,
    candle_close_ts,
    resolve_execution_reference,
)
from .experiment_manifest import ExecutionTimingPolicy
from .metrics import ResearchMetrics


START_CASH_KRW = 1_000_000.0
BUY_FRACTION = 0.99


@dataclass(frozen=True)
class BacktestRun:
    metrics: ResearchMetrics
    trades: tuple[dict[str, object], ...]
    candle_count: int
    warnings: tuple[str, ...]
    regime_performance: tuple[RegimePerformanceRow, ...] = ()
    regime_coverage: tuple[RegimeCoverageRow, ...] = ()
    execution_event_summary: dict[str, object] | None = None
    decisions: tuple[dict[str, object], ...] = ()


@dataclass
class _PendingFill:
    fill: ExecutionFill
    trade_index: int
    side: str
    effective_ts: int
    qty: float
    fee: float
    slippage: float
    cash_delta: float
    entry_regime_snapshot: dict[str, object] | None = None
    exit_regime_snapshot: dict[str, object] | None = None


def run_sma_backtest(
    *,
    dataset: DatasetSnapshot,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
    execution_timing_policy: ExecutionTimingPolicy | None = None,
) -> BacktestRun:
    short_n = int(parameter_values.get("SMA_SHORT", parameter_values.get("short_n", 0)))
    long_n = int(parameter_values.get("SMA_LONG", parameter_values.get("long_n", 0)))
    min_gap = float(
        parameter_values.get(
            "SMA_FILTER_GAP_MIN_RATIO",
            parameter_values.get("strategy_min_expected_edge_ratio", 0.0),
        )
    )
    min_range = float(parameter_values.get("SMA_FILTER_VOL_MIN_RANGE_RATIO", 0.0))
    if short_n <= 0 or long_n <= 0 or short_n >= long_n:
        raise ValueError("SMA_SHORT must be smaller than SMA_LONG")

    candles = dataset.candles
    warnings: list[str] = []
    if len(candles) < long_n + 2:
        return BacktestRun(
            metrics=_empty_metrics(parameter_stability_score),
            trades=(),
            candle_count=len(candles),
            warnings=("not_enough_candles",),
            regime_performance=(),
            regime_coverage=(),
            execution_event_summary=empty_execution_event_summary(),
        )

    closes = [candle.close for candle in candles]
    regime_snapshots: list[dict[str, object]] = []
    thresholds = MarketRegimeThresholds(
        min_trend_strength_ratio=max(0.0, min_gap),
        low_volatility_ratio=max(0.0, min_range),
    )
    cash = START_CASH_KRW
    qty = 0.0
    entry_cost_basis = 0.0
    entry_regime_snapshot: dict[str, object] | None = None
    entry_fee = 0.0
    entry_slippage = 0.0
    peak = START_CASH_KRW
    max_drawdown = 0.0
    fee_total = 0.0
    slippage_total = 0.0
    trades: list[dict[str, object]] = []
    pending_fills: list[_PendingFill] = []
    decisions: list[dict[str, object]] = []
    closed_pnls: list[float] = []
    prev_above: bool | None = None
    model = execution_model or FixedBpsExecutionModel(fee_rate=fee_rate, slippage_bps=slippage_bps)
    timing_policy = execution_timing_policy or ExecutionTimingPolicy()

    for index in range(long_n, len(candles)):
        candle = candles[index]
        mark_boundary_ts = candle_close_ts(candle, interval=dataset.interval)
        decision_boundary_ts = mark_boundary_ts + int(timing_policy.decision_guard_ms)
        cash, qty, entry_cost_basis, entry_regime_snapshot, entry_fee, entry_slippage, fee_total, slippage_total = _apply_pending_fills(
            pending_fills=pending_fills,
            trades=trades,
            boundary_ts=mark_boundary_ts,
            cash=cash,
            qty=qty,
            entry_cost_basis=entry_cost_basis,
            entry_regime_snapshot=entry_regime_snapshot,
            entry_fee=entry_fee,
            entry_slippage=entry_slippage,
            fee_total=fee_total,
            slippage_total=slippage_total,
            closed_pnls=closed_pnls,
        )
        mark_cash = cash
        mark_qty = qty
        cash, qty, entry_cost_basis, entry_regime_snapshot, entry_fee, entry_slippage, fee_total, slippage_total = _apply_pending_fills(
            pending_fills=pending_fills,
            trades=trades,
            boundary_ts=decision_boundary_ts,
            cash=cash,
            qty=qty,
            entry_cost_basis=entry_cost_basis,
            entry_regime_snapshot=entry_regime_snapshot,
            entry_fee=entry_fee,
            entry_slippage=entry_slippage,
            fee_total=fee_total,
            slippage_total=slippage_total,
            closed_pnls=closed_pnls,
        )
        prev_short = _sma(closes, short_n, index)
        prev_long = _sma(closes, long_n, index)
        curr_short = _sma(closes, short_n, index + 1)
        curr_long = _sma(closes, long_n, index + 1)
        above = curr_short > curr_long
        gap_ratio = abs(curr_short - curr_long) / curr_long if curr_long > 0.0 else 0.0
        range_ratio = (candle.high - candle.low) / candle.close if candle.close > 0.0 else 0.0
        regime_snapshot = classify_market_regime(
            candles=candles[: index + 1],
            short_sma=curr_short,
            long_sma=curr_long,
            volatility_window=max(1, int(parameter_values.get("SMA_FILTER_VOL_WINDOW", 10))),
            thresholds=thresholds,
            overextended_lookback=max(1, int(parameter_values.get("SMA_FILTER_OVEREXT_LOOKBACK", 3))),
            overextended_max_return_ratio=float(parameter_values.get("SMA_FILTER_OVEREXT_MAX_RETURN_RATIO", 0.0)),
        ).as_dict()
        regime_snapshots.append(regime_snapshot)

        action = "HOLD"
        raw_signal = "HOLD"
        raw_reason = "sma no crossover"
        pending_buy_qty = sum(item.qty for item in pending_fills if item.side == "BUY")
        pending_sell_qty = sum(item.qty for item in pending_fills if item.side == "SELL")
        sellable_qty = max(0.0, qty - pending_sell_qty)
        filter_blocked = False
        blocked_filters: list[str] = []
        if prev_above is not None:
            if not prev_above and above:
                raw_signal = "BUY"
                raw_reason = "sma golden cross"
            elif prev_above and not above:
                raw_signal = "SELL"
                raw_reason = "sma dead cross"
        if raw_signal in {"BUY", "SELL"} and gap_ratio < min_gap:
            filter_blocked = True
            blocked_filters.append("gap")
        if raw_signal in {"BUY", "SELL"} and range_ratio < min_range:
            filter_blocked = True
            blocked_filters.append("volatility")
        if not filter_blocked and prev_above is not None:
            if raw_signal == "BUY" and qty <= 0.0 and pending_buy_qty <= 0.0:
                action = "BUY"
            elif raw_signal == "SELL" and sellable_qty > 0.0:
                action = "SELL"
        decisions.append(
            _research_decision_payload(
                dataset=dataset,
                parameter_values=parameter_values,
                fee_rate=fee_rate,
                slippage_bps=slippage_bps,
                timing_policy=timing_policy,
                candle_ts=int(candle.ts),
                decision_ts=int(decision_boundary_ts),
                raw_signal=raw_signal,
                final_signal=action,
                raw_reason=raw_reason,
                blocked=bool(raw_signal in {"BUY", "SELL"} and action == "HOLD"),
                blocked_filters=blocked_filters,
                prev_s=prev_short,
                prev_l=prev_long,
                curr_s=curr_short,
                curr_l=curr_long,
                gap_ratio=gap_ratio,
                range_ratio=range_ratio,
                regime_snapshot=regime_snapshot,
                qty=qty,
                sellable_qty=sellable_qty,
            )
        )

        if action == "BUY":
            signal = build_signal_event(
                candle=candle,
                interval=dataset.interval,
                side="BUY",
                policy=timing_policy,
                feature_snapshot=_feature_snapshot(
                    short_sma=curr_short,
                    long_sma=curr_long,
                    gap_ratio=gap_ratio,
                    range_ratio=range_ratio,
                    index=index,
                ),
                regime_snapshot=regime_snapshot,
            )
            reference = resolve_execution_reference(
                dataset=dataset,
                signal=signal,
                signal_index=index,
                policy=timing_policy,
                model_latency_ms=_model_latency_ms(model),
            )
            if reference.fill_reference_price is None:
                fill = _failed_fill(
                    model=model,
                    signal=signal,
                    reference=reference,
                    timing_policy=timing_policy,
                    side="BUY",
                    fee_rate=fee_rate,
                    requested_notional=cash * BUY_FRACTION,
                )
                warnings.extend(_execution_reference_warnings(fill))
                trades.append(_trade_from_fill(fill, cash=cash, asset_qty=qty, pnl=None))
                prev_above = above
                continue
            spend = cash * BUY_FRACTION
            fill = model.simulate(
                ExecutionRequest(
                    signal_ts=signal.signal_candle_start_ts,
                    decision_ts=signal.decision_ts,
                    side="BUY",
                    reference_price=float(reference.fill_reference_price),
                    requested_notional=spend,
                    fee_rate=fee_rate,
                    **_timing_request_fields(signal, reference, timing_policy),
                )
            )
            warnings.extend(_execution_reference_warnings(fill))
            if fill.fill_status == "failed" or fill.avg_fill_price is None or fill.filled_qty <= 0.0:
                trades.append(_trade_from_fill(fill, cash=cash, asset_qty=qty, pnl=None))
                prev_above = above
                continue
            exec_price = float(fill.avg_fill_price)
            fee = fill.fee
            received_qty = fill.filled_qty
            actual_spend = (exec_price * received_qty) + fee
            reference_cost = float(fill.reference_price) * received_qty
            slipped_cost = exec_price * received_qty
            buy_slippage = max(0.0, slipped_cost - reference_cost)
            pending = _PendingFill(
                fill=fill,
                trade_index=len(trades),
                side="BUY",
                effective_ts=_fill_effective_ts(fill),
                qty=received_qty,
                fee=fee,
                slippage=buy_slippage,
                cash_delta=-actual_spend,
                entry_regime_snapshot=dict(regime_snapshot),
            )
            trades.append(_pending_trade_from_fill(fill, cash=cash, asset_qty=qty))
            if pending.effective_ts <= mark_boundary_ts:
                mark_cash += pending.cash_delta
                mark_qty += pending.qty
            pending_fills.append(pending)
            cash, qty, entry_cost_basis, entry_regime_snapshot, entry_fee, entry_slippage, fee_total, slippage_total = _apply_pending_fills(
                pending_fills=pending_fills,
                trades=trades,
                boundary_ts=decision_boundary_ts,
                cash=cash,
                qty=qty,
                entry_cost_basis=entry_cost_basis,
                entry_regime_snapshot=entry_regime_snapshot,
                entry_fee=entry_fee,
                entry_slippage=entry_slippage,
                fee_total=fee_total,
                slippage_total=slippage_total,
                closed_pnls=closed_pnls,
            )
        elif action == "SELL":
            signal = build_signal_event(
                candle=candle,
                interval=dataset.interval,
                side="SELL",
                policy=timing_policy,
                feature_snapshot=_feature_snapshot(
                    short_sma=curr_short,
                    long_sma=curr_long,
                    gap_ratio=gap_ratio,
                    range_ratio=range_ratio,
                    index=index,
                ),
                regime_snapshot=regime_snapshot,
            )
            reference = resolve_execution_reference(
                dataset=dataset,
                signal=signal,
                signal_index=index,
                policy=timing_policy,
                model_latency_ms=_model_latency_ms(model),
            )
            if reference.fill_reference_price is None:
                fill = _failed_fill(
                    model=model,
                    signal=signal,
                    reference=reference,
                    timing_policy=timing_policy,
                    side="SELL",
                    fee_rate=fee_rate,
                    requested_qty=sellable_qty,
                )
                warnings.extend(_execution_reference_warnings(fill))
                trades.append(_trade_from_fill(fill, cash=cash, asset_qty=qty, pnl=None))
                prev_above = above
                continue
            fill = model.simulate(
                ExecutionRequest(
                    signal_ts=signal.signal_candle_start_ts,
                    decision_ts=signal.decision_ts,
                    side="SELL",
                    reference_price=float(reference.fill_reference_price),
                    requested_qty=sellable_qty,
                    fee_rate=fee_rate,
                    **_timing_request_fields(signal, reference, timing_policy),
                )
            )
            warnings.extend(_execution_reference_warnings(fill))
            if fill.fill_status == "failed" or fill.avg_fill_price is None or fill.filled_qty <= 0.0:
                trades.append(_trade_from_fill(fill, cash=cash, asset_qty=qty, pnl=None))
                prev_above = above
                continue
            exec_price = float(fill.avg_fill_price)
            sell_qty = fill.filled_qty
            gross = sell_qty * exec_price
            fee = fill.fee
            reference_proceeds = float(fill.reference_price) * sell_qty
            sell_slippage = max(0.0, reference_proceeds - gross)
            net_proceeds = gross - fee
            pending = _PendingFill(
                fill=fill,
                trade_index=len(trades),
                side="SELL",
                effective_ts=_fill_effective_ts(fill),
                qty=sell_qty,
                fee=fee,
                slippage=sell_slippage,
                cash_delta=net_proceeds,
                entry_regime_snapshot=entry_regime_snapshot,
                exit_regime_snapshot=dict(regime_snapshot),
            )
            trades.append(_pending_trade_from_fill(fill, cash=cash, asset_qty=qty))
            if pending.effective_ts <= mark_boundary_ts:
                mark_cash += pending.cash_delta
                mark_qty = max(0.0, mark_qty - pending.qty)
            pending_fills.append(pending)
            cash, qty, entry_cost_basis, entry_regime_snapshot, entry_fee, entry_slippage, fee_total, slippage_total = _apply_pending_fills(
                pending_fills=pending_fills,
                trades=trades,
                boundary_ts=decision_boundary_ts,
                cash=cash,
                qty=qty,
                entry_cost_basis=entry_cost_basis,
                entry_regime_snapshot=entry_regime_snapshot,
                entry_fee=entry_fee,
                entry_slippage=entry_slippage,
                fee_total=fee_total,
                slippage_total=slippage_total,
                closed_pnls=closed_pnls,
            )

        equity = mark_cash + mark_qty * candle.close
        peak = max(peak, equity)
        if peak > 0.0:
            max_drawdown = max(max_drawdown, (peak - equity) / peak)
        prev_above = above

    last = candles[-1]
    last_mark_ts = candle_close_ts(last, interval=dataset.interval)
    cash, qty, entry_cost_basis, entry_regime_snapshot, entry_fee, entry_slippage, fee_total, slippage_total = _apply_pending_fills(
        pending_fills=pending_fills,
        trades=trades,
        boundary_ts=last_mark_ts,
        cash=cash,
        qty=qty,
        entry_cost_basis=entry_cost_basis,
        entry_regime_snapshot=entry_regime_snapshot,
        entry_fee=entry_fee,
        entry_slippage=entry_slippage,
        fee_total=fee_total,
        slippage_total=slippage_total,
        closed_pnls=closed_pnls,
    )
    _mark_pending_fills_at_end(pending_fills=pending_fills, trades=trades, final_mark_ts=last_mark_ts)
    final_equity = cash + qty * last.close
    return_pct = ((final_equity / START_CASH_KRW) - 1.0) * 100.0
    metrics = _metrics(
        return_pct=return_pct,
        max_drawdown_pct=max_drawdown * 100.0,
        closed_pnls=closed_pnls,
        fee_total=fee_total,
        slippage_total=slippage_total,
        parameter_stability_score=parameter_stability_score,
    )
    coverage = aggregate_regime_coverage(snapshots=regime_snapshots, trades=trades)
    performance = aggregate_regime_performance(trades=trades, coverage=coverage, start_cash=START_CASH_KRW)
    execution_summary = execution_event_summary(trades)
    return BacktestRun(
        metrics=metrics,
        trades=tuple(trades),
        candle_count=len(candles),
        warnings=tuple(warnings),
        regime_performance=performance,
        regime_coverage=coverage,
        execution_event_summary=execution_summary,
        decisions=tuple(decisions),
    )


def _sma(values: list[float], n: int, end: int) -> float:
    return sum(values[end - n : end]) / n


def _apply_pending_fills(
    *,
    pending_fills: list[_PendingFill],
    trades: list[dict[str, object]],
    boundary_ts: int,
    cash: float,
    qty: float,
    entry_cost_basis: float,
    entry_regime_snapshot: dict[str, object] | None,
    entry_fee: float,
    entry_slippage: float,
    fee_total: float,
    slippage_total: float,
    closed_pnls: list[float],
) -> tuple[float, float, float, dict[str, object] | None, float, float, float, float]:
    ready = sorted(
        [item for item in pending_fills if item.effective_ts <= int(boundary_ts)],
        key=lambda item: (item.effective_ts, item.trade_index),
    )
    for pending in ready:
        pending_fills.remove(pending)
        trade = trades[pending.trade_index]
        fill = pending.fill
        if pending.side == "BUY":
            cash += pending.cash_delta
            qty += pending.qty
            entry_cost_basis = abs(pending.cash_delta)
            entry_regime_snapshot = pending.entry_regime_snapshot
            entry_fee = pending.fee
            entry_slippage = pending.slippage
            fee_total += pending.fee
            slippage_total += pending.slippage
            _mark_trade_applied(
                trade,
                cash=cash,
                asset_qty=qty,
                pnl=None,
                entry_regime_snapshot=entry_regime_snapshot,
                exit_regime_snapshot=None,
                net_pnl=None,
                fee_total=pending.fee,
                slippage_total=pending.slippage,
            )
        else:
            filled_fraction = pending.qty / qty if qty > 0.0 else 0.0
            pnl = pending.cash_delta - (entry_cost_basis * filled_fraction)
            cash += pending.cash_delta
            qty = max(0.0, qty - pending.qty)
            entry_cost_basis = entry_cost_basis * (1.0 - filled_fraction) if qty > 0.0 else 0.0
            fee_total += pending.fee
            slippage_total += pending.slippage
            if fill.fill_status in {"filled", "partial"}:
                closed_pnls.append(pnl)
            trade_fee_total = entry_fee + pending.fee
            trade_slippage_total = entry_slippage + pending.slippage
            _mark_trade_applied(
                trade,
                cash=cash,
                asset_qty=qty,
                pnl=pnl,
                entry_regime_snapshot=pending.entry_regime_snapshot,
                exit_regime_snapshot=pending.exit_regime_snapshot,
                net_pnl=pnl,
                fee_total=trade_fee_total,
                slippage_total=trade_slippage_total,
            )
            if qty <= 0.0:
                entry_regime_snapshot = None
                entry_fee = 0.0
                entry_slippage = 0.0
    return cash, qty, entry_cost_basis, entry_regime_snapshot, entry_fee, entry_slippage, fee_total, slippage_total


def _timing_request_fields(
    signal: SignalEvent,
    reference: ExecutionReferenceEvent,
    policy: ExecutionTimingPolicy,
) -> dict[str, object]:
    fields = reference.request_fields()
    fields.update(
        {
            "signal_candle_start_ts": signal.signal_candle_start_ts,
            "signal_candle_close_ts": signal.signal_candle_close_ts,
            "signal_reference_price": signal.signal_reference_price,
            "signal_reference_source": signal.signal_reference_source,
            "allow_same_candle_close_fill": policy.allow_same_candle_close_fill,
            "quote_selection": policy.quote_selection,
            "fill_reference_policy": policy.fill_reference_policy,
            "top_of_book_source": reference.quote_source,
            "feature_snapshot": signal.feature_snapshot,
            "regime_snapshot": signal.regime_snapshot,
        }
    )
    return fields


def _feature_snapshot(
    *,
    short_sma: float,
    long_sma: float,
    gap_ratio: float,
    range_ratio: float,
    index: int,
) -> dict[str, object]:
    return {
        "short_sma": float(short_sma),
        "long_sma": float(long_sma),
        "gap_ratio": float(gap_ratio),
        "range_ratio": float(range_ratio),
        "candle_index": int(index),
    }


def _research_decision_payload(
    *,
    dataset: DatasetSnapshot,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    timing_policy: ExecutionTimingPolicy,
    candle_ts: int,
    decision_ts: int,
    raw_signal: str,
    final_signal: str,
    raw_reason: str,
    blocked: bool,
    blocked_filters: list[str],
    prev_s: float,
    prev_l: float,
    curr_s: float,
    curr_l: float,
    gap_ratio: float,
    range_ratio: float,
    regime_snapshot: dict[str, object],
    qty: float,
    sellable_qty: float,
) -> dict[str, object]:
    flat_no_position = float(qty) <= 0.0 and float(sellable_qty) <= 0.0
    position_state = (
        {
            "comparison_state": "flat_no_dust_no_position",
            "entry_allowed": True,
            "exit_allowed": False,
            "dust_state": "flat",
            "effective_flat": True,
            "normalized_exposure_active": False,
        }
        if flat_no_position
        else {
            "research_position_model": "cash_qty_simulation_v1",
            "unsupported_reason": "research_model_lacks_lot_native_authority",
            "qty": float(qty),
            "sellable_qty": float(sellable_qty),
        }
    )
    order_rules = {
        "source": "research_execution_model",
        "fee_rate": float(fee_rate),
        "slippage_bps": float(slippage_bps),
        "sizing": "cash_fraction_0.99_or_full_sellable_qty",
    }
    return {
        "strategy_name": "sma_with_filter",
        "market": dataset.market,
        "interval": dataset.interval,
        "signal_timestamp": str(candle_ts),
        "candle_ts": int(candle_ts),
        "through_ts_ms": int(candle_ts),
        "candle_basis": "research_closed_candle",
        "decision_ts": int(decision_ts),
        "raw_signal": raw_signal,
        "final_signal": final_signal,
        "side": final_signal,
        "blocked": bool(blocked),
        "block_reason": f"filtered entry: {', '.join(blocked_filters)}" if blocked_filters else (raw_reason if blocked else ""),
        "blocked_filters": tuple(blocked_filters),
        "prev_s": float(prev_s),
        "prev_l": float(prev_l),
        "curr_s": float(curr_s),
        "curr_l": float(curr_l),
        "feature_hash": canonical_payload_hash(
            {
                "prev_s": prev_s,
                "prev_l": prev_l,
                "curr_s": curr_s,
                "curr_l": curr_l,
                "gap_ratio": gap_ratio,
                "range_ratio": range_ratio,
            }
        ),
        "gap_ratio": float(gap_ratio),
        "range_ratio": float(range_ratio),
        "expected_edge_ratio": float(gap_ratio),
        "required_edge_ratio": float(
            parameter_values.get(
                "SMA_FILTER_GAP_MIN_RATIO",
                parameter_values.get("strategy_min_expected_edge_ratio", 0.0),
            )
        ),
        "fee_authority_hash": canonical_payload_hash({"source": "research_manifest", "fee_rate": float(fee_rate)}),
        "fee_model_hash": canonical_payload_hash({"fee_rate": float(fee_rate)}),
        "slippage_model_hash": canonical_payload_hash({"slippage_bps": float(slippage_bps)}),
        "order_rules_hash": canonical_payload_hash(order_rules),
        "market_regime": str(regime_snapshot.get("composite_regime") or ""),
        "regime_decision": "allowed",
        "regime_block_reason": "",
        "position_state_hash": canonical_flat_position_state_hash()
        if flat_no_position
        else canonical_payload_hash(position_state),
        "entry_allowed": bool(qty <= 0.0),
        "exit_allowed": bool(sellable_qty > 0.0),
        "dust_state": "flat" if flat_no_position else "research_not_modeled",
        "effective_flat": bool(qty <= 0.0),
        "normalized_exposure_active": bool(qty > 0.0),
        "exit_rule": "opposite_cross" if final_signal == "SELL" and raw_signal == "SELL" else "",
        "exit_reason": "exit by opposite cross" if final_signal == "SELL" and raw_signal == "SELL" else "",
        "exit_evaluations_hash": canonical_payload_hash(
            {"raw_signal": raw_signal, "final_signal": final_signal, "position_qty": float(qty)}
        ),
        "execution_timing_policy_hash": canonical_payload_hash(timing_policy.as_dict()),
        "replay_fingerprint_hash": canonical_payload_hash(
            {
                "dataset_content_hash": dataset.content_hash(),
                "parameter_values": parameter_values,
                "candle_ts": int(candle_ts),
            }
        ),
    }


def _model_latency_ms(model: ExecutionModel) -> int:
    try:
        return int(getattr(model, "latency_ms", 0) or 0)
    except (TypeError, ValueError):
        return 0


def _failed_fill(
    *,
    model: ExecutionModel,
    signal: SignalEvent,
    reference: ExecutionReferenceEvent,
    timing_policy: ExecutionTimingPolicy,
    side: str,
    fee_rate: float,
    requested_qty: float | None = None,
    requested_notional: float | None = None,
) -> ExecutionFill:
    request_qty = float(requested_qty or 0.0)
    if request_qty <= 0.0 and requested_notional is not None and signal.signal_reference_price > 0:
        request_qty = float(requested_notional) / float(signal.signal_reference_price)
    return ExecutionFill(
        signal_ts=signal.signal_candle_start_ts,
        decision_ts=signal.decision_ts,
        submit_ts_assumption=reference.submit_ts_assumption,
        side=str(side).upper(),
        order_type="market",
        reference_price=float(signal.signal_reference_price),
        fill_reference_ts=reference.fill_reference_ts,
        fill_reference_price=reference.fill_reference_price,
        fill_reference_source=reference.fill_reference_source,
        signal_candle_start_ts=signal.signal_candle_start_ts,
        signal_candle_close_ts=signal.signal_candle_close_ts,
        signal_reference_price=signal.signal_reference_price,
        signal_reference_source=signal.signal_reference_source,
        quote_ts=reference.quote_ts,
        quote_age_ms=reference.quote_age_ms,
        quote_source=reference.quote_source,
        requested_qty=request_qty,
        filled_qty=0.0,
        remaining_qty=request_qty,
        avg_fill_price=None,
        fee=0.0,
        slippage_bps=0.0,
        latency_ms=_model_latency_ms(model),
        fill_status=_failed_fill_status(reference.failure_reason),
        model_name=getattr(model, "name", "unknown"),
        model_version=getattr(model, "version", "unknown"),
        model_params_hash=model_params_hash(model.params_payload()),
        best_bid=reference.best_bid,
        best_ask=reference.best_ask,
        spread_bps=reference.spread_bps,
        execution_reality_level=reference.execution_reality_level,
        allow_same_candle_close_fill=timing_policy.allow_same_candle_close_fill,
        quote_selection=timing_policy.quote_selection,
        fill_reference_policy=timing_policy.fill_reference_policy,
        top_of_book_source=reference.quote_source,
        top_of_book_is_full_depth=reference.top_of_book_is_full_depth,
        execution_reference_failure_reason=reference.failure_reason,
        latency_applied_to_reference=reference.latency_applied_to_reference,
        latency_applied_to_submit_ts=reference.latency_applied_to_submit_ts,
        latency_applied_to_fill_reference=reference.latency_applied_to_fill_reference,
        latency_reference_policy_warning=reference.latency_reference_policy_warning,
        feature_snapshot=signal.feature_snapshot,
        regime_snapshot=signal.regime_snapshot,
        intra_candle_policy=reference.intra_candle_policy,
    )


def _failed_fill_status(reason: str | None) -> str:
    if reason == "missing_quote_skipped":
        return "skipped"
    if reason == "missing_quote_warning":
        return "skipped_with_warning"
    return "failed"


def _trade(
    ts: int,
    side: str,
    price: float,
    qty: float,
    fee: float,
    cash: float,
    asset_qty: float,
    pnl: float | None,
    *,
    entry_regime_snapshot: dict[str, object] | None = None,
    exit_regime_snapshot: dict[str, object] | None = None,
    net_pnl: float | None = None,
    fee_total: float | None = None,
    slippage_total: float | None = None,
) -> dict[str, object]:
    entry_regime = None
    if entry_regime_snapshot is not None:
        entry_regime = entry_regime_snapshot.get("composite_regime")
    exit_regime = None
    if exit_regime_snapshot is not None:
        exit_regime = exit_regime_snapshot.get("composite_regime")
    return {
        "ts": int(ts),
        "side": side,
        "price": float(price),
        "qty": float(qty),
        "fee": float(fee),
        "cash": float(cash),
        "asset_qty": float(asset_qty),
        "closed_trade_pnl": pnl,
        "net_pnl": net_pnl,
        "fee_total": fee_total,
        "slippage_total": slippage_total,
        "entry_regime": entry_regime,
        "exit_regime": exit_regime,
        "entry_regime_snapshot": entry_regime_snapshot,
        "exit_regime_snapshot": exit_regime_snapshot,
    }


def _trade_from_fill(
    fill: Any,
    *,
    cash: float,
    asset_qty: float,
    pnl: float | None,
    entry_regime_snapshot: dict[str, object] | None = None,
    exit_regime_snapshot: dict[str, object] | None = None,
    net_pnl: float | None = None,
    fee_total: float | None = None,
    slippage_total: float | None = None,
) -> dict[str, object]:
    trade = _trade(
        fill.signal_ts,
        fill.side,
        float(fill.avg_fill_price) if fill.avg_fill_price is not None else float(fill.reference_price),
        float(fill.filled_qty),
        float(fill.fee),
        cash,
        asset_qty,
        pnl,
        entry_regime_snapshot=entry_regime_snapshot,
        exit_regime_snapshot=exit_regime_snapshot,
        net_pnl=net_pnl,
        fee_total=fee_total,
        slippage_total=slippage_total,
    )
    trade["signal_ts"] = fill.signal_ts
    trade["decision_ts"] = fill.decision_ts
    trade["submit_ts_assumption"] = fill.submit_ts_assumption
    trade["fill_ts"] = fill.fill_reference_ts
    trade["fill_reference_ts"] = fill.fill_reference_ts
    trade["event_ts_role"] = "signal_ts_legacy"
    trade["execution"] = fill.as_dict()
    _annotate_execution_record_type(trade, fill)
    trade["portfolio_effective_ts"] = fill.fill_reference_ts
    _annotate_portfolio_application(trade, fill, portfolio_applied=bool(trade["is_execution_filled"]))
    return trade


def _pending_trade_from_fill(fill: Any, *, cash: float, asset_qty: float) -> dict[str, object]:
    trade = _trade_from_fill(fill, cash=cash, asset_qty=asset_qty, pnl=None)
    trade["portfolio_effective_ts"] = _fill_effective_ts(fill)
    _annotate_portfolio_application(trade, fill, portfolio_applied=False)
    return trade


def _mark_trade_applied(
    trade: dict[str, object],
    *,
    cash: float,
    asset_qty: float,
    pnl: float | None,
    entry_regime_snapshot: dict[str, object] | None,
    exit_regime_snapshot: dict[str, object] | None,
    net_pnl: float | None,
    fee_total: float | None,
    slippage_total: float | None,
) -> None:
    entry_regime = entry_regime_snapshot.get("composite_regime") if entry_regime_snapshot is not None else None
    exit_regime = exit_regime_snapshot.get("composite_regime") if exit_regime_snapshot is not None else None
    trade.update(
        {
            "cash": float(cash),
            "asset_qty": float(asset_qty),
            "closed_trade_pnl": pnl,
            "net_pnl": net_pnl,
            "fee_total": fee_total,
            "slippage_total": slippage_total,
            "entry_regime": entry_regime,
            "exit_regime": exit_regime,
            "entry_regime_snapshot": entry_regime_snapshot,
            "exit_regime_snapshot": exit_regime_snapshot,
        }
    )
    _annotate_portfolio_application(trade, trade.get("execution") or {}, portfolio_applied=True)


def _fill_effective_ts(fill: Any) -> int:
    if fill.fill_reference_ts is not None:
        return int(fill.fill_reference_ts)
    return int(fill.submit_ts_assumption)


def _annotate_execution_record_type(trade: dict[str, object], fill: Any) -> None:
    status = str(getattr(fill, "fill_status", ""))
    is_filled = float(getattr(fill, "filled_qty", 0.0) or 0.0) > 0.0 and status in {"filled", "partial"}
    is_skipped = status in {"skipped", "skipped_with_warning"}
    is_failed = status == "failed"
    if is_skipped:
        record_type = "skipped_execution"
    elif is_failed:
        record_type = "failed_execution"
    elif is_filled:
        record_type = "portfolio_trade"
    else:
        record_type = "execution_attempt"
    trade["record_type"] = record_type
    trade["is_execution_attempt"] = True
    trade["is_execution_filled"] = is_filled
    trade["is_filled_trade"] = is_filled
    trade["is_skipped_execution"] = is_skipped
    trade["is_failed_execution"] = is_failed
    trade["is_portfolio_applied_trade"] = is_filled
    trade["is_effective_trade"] = is_filled
    trade["portfolio_application_status"] = "applied" if is_filled else "not_applicable"


def _annotate_portfolio_application(
    trade: dict[str, object],
    fill: Any,
    *,
    portfolio_applied: bool,
) -> None:
    if isinstance(fill, dict):
        status = str(fill.get("fill_status") or "")
        filled_qty = float(fill.get("filled_qty") or 0.0)
    else:
        status = str(getattr(fill, "fill_status", ""))
        filled_qty = float(getattr(fill, "filled_qty", 0.0) or 0.0)
    is_execution_filled = filled_qty > 0.0 and status in {"filled", "partial"}
    is_skipped = status in {"skipped", "skipped_with_warning"}
    is_failed = status == "failed"
    is_portfolio_trade = bool(is_execution_filled and portfolio_applied)
    if is_portfolio_trade:
        record_type = "portfolio_trade"
        application_status = "applied"
    elif is_execution_filled:
        record_type = "pending_execution"
        application_status = "pending"
    elif is_skipped:
        record_type = "skipped_execution"
        application_status = "not_applicable"
    elif is_failed:
        record_type = "failed_execution"
        application_status = "not_applicable"
    else:
        record_type = "execution_attempt"
        application_status = "not_applicable"
    trade.update(
        {
            "record_type": record_type,
            "is_execution_attempt": True,
            "is_execution_filled": is_execution_filled,
            "is_portfolio_applied_trade": is_portfolio_trade,
            "is_effective_trade": is_portfolio_trade,
            "is_filled_trade": is_portfolio_trade,
            "is_skipped_execution": is_skipped,
            "is_failed_execution": is_failed,
            "portfolio_applied": is_portfolio_trade,
            "portfolio_application_status": application_status,
        }
    )


def _mark_pending_fills_at_end(
    *,
    pending_fills: list[_PendingFill],
    trades: list[dict[str, object]],
    final_mark_ts: int,
) -> None:
    for pending in pending_fills:
        trade = trades[pending.trade_index]
        trade["pending_execution_at_end"] = True
        trade["pending_execution_after_dataset_end"] = int(pending.effective_ts) > int(final_mark_ts)
        trade["dataset_final_mark_ts"] = int(final_mark_ts)


def execution_event_summary(trades: Any) -> dict[str, object]:
    rows = [trade for trade in trades if isinstance(trade, dict)]
    attempts = [trade for trade in rows if bool(trade.get("is_execution_attempt"))]
    execution_filled = [trade for trade in rows if bool(trade.get("is_execution_filled"))]
    portfolio_applied = [trade for trade in rows if bool(trade.get("is_portfolio_applied_trade"))]
    pending = [
        trade
        for trade in rows
        if bool(trade.get("is_execution_filled")) and not bool(trade.get("is_portfolio_applied_trade"))
    ]
    skipped = [trade for trade in rows if bool(trade.get("is_skipped_execution"))]
    failed = [trade for trade in rows if bool(trade.get("is_failed_execution"))]
    closed = [
        trade
        for trade in portfolio_applied
        if str(trade.get("side") or "").upper() == "SELL"
    ]
    pending_at_end = [trade for trade in pending if bool(trade.get("pending_execution_at_end"))]
    pending_after_end = [trade for trade in pending if bool(trade.get("pending_execution_after_dataset_end"))]
    return {
        "execution_attempt_count": len(attempts),
        "execution_filled_count": len(execution_filled),
        "filled_execution_count": len(execution_filled),
        "portfolio_applied_trade_count": len(portfolio_applied),
        "pending_execution_count": len(pending),
        "skipped_execution_count": len(skipped),
        "failed_execution_count": len(failed),
        "closed_trade_count": len(closed),
        "pending_execution_at_end_count": len(pending_at_end),
        "pending_execution_after_dataset_end_count": len(pending_after_end),
        "execution_event_timeline_incomplete": bool(pending_after_end),
    }


def empty_execution_event_summary() -> dict[str, object]:
    return execution_event_summary(())


def _execution_reference_warnings(fill: Any) -> list[str]:
    warnings: list[str] = []
    if getattr(fill, "execution_reference_failure_reason", None) == "missing_quote_warning":
        warnings.append("missing_quote_warning")
    if getattr(fill, "latency_reference_policy_warning", None):
        warnings.append(str(fill.latency_reference_policy_warning))
    return warnings


def _empty_metrics(parameter_stability_score: float | None) -> ResearchMetrics:
    return ResearchMetrics(
        return_pct=0.0,
        max_drawdown_pct=0.0,
        profit_factor=None,
        trade_count=0,
        win_rate=0.0,
        avg_win=None,
        avg_loss=None,
        fee_total=0.0,
        slippage_total=0.0,
        max_consecutive_losses=0,
        single_trade_dependency_score=None,
        parameter_stability_score=parameter_stability_score,
    )


def _metrics(
    *,
    return_pct: float,
    max_drawdown_pct: float,
    closed_pnls: list[float],
    fee_total: float,
    slippage_total: float,
    parameter_stability_score: float | None,
) -> ResearchMetrics:
    wins = [pnl for pnl in closed_pnls if pnl > 0.0]
    losses = [pnl for pnl in closed_pnls if pnl < 0.0]
    profit_factor = (sum(wins) / abs(sum(losses))) if losses else (float("inf") if wins else None)
    largest_abs = max((abs(pnl) for pnl in closed_pnls), default=0.0)
    total_abs = sum(abs(pnl) for pnl in closed_pnls)
    return ResearchMetrics(
        return_pct=float(return_pct),
        max_drawdown_pct=float(max_drawdown_pct),
        profit_factor=profit_factor,
        trade_count=len(closed_pnls),
        win_rate=(len(wins) / len(closed_pnls)) if closed_pnls else 0.0,
        avg_win=(sum(wins) / len(wins)) if wins else None,
        avg_loss=(sum(losses) / len(losses)) if losses else None,
        fee_total=float(fee_total),
        slippage_total=float(slippage_total),
        max_consecutive_losses=_max_consecutive_losses(closed_pnls),
        single_trade_dependency_score=(largest_abs / total_abs) if total_abs > 0.0 else None,
        parameter_stability_score=parameter_stability_score,
    )


def _max_consecutive_losses(values: list[float]) -> int:
    longest = 0
    current = 0
    for value in values:
        if value < 0.0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest
