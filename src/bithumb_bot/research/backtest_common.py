from __future__ import annotations

from typing import Any

from bithumb_bot.canonical_decision import canonical_payload_hash
from bithumb_bot.position_authority import research_position_authority_snapshot

from .dataset_snapshot import DatasetSnapshot
from .execution_model import ExecutionFill, ExecutionModel, model_params_hash
from .execution_timing import ExecutionReferenceEvent, SignalEvent
from .experiment_manifest import ExecutionTimingPolicy, PortfolioPolicy
from .metrics import ResearchMetrics
from .metrics_contract import (
    ClosedTradeRecord,
    EquityPoint,
    ExecutionRecord,
    MetricContractV2,
    PositionInterval,
    build_metrics_v2,
)
from .backtest_types import BacktestRunContext
from .backtest_support import BacktestAccumulator, PendingFill

def _create_exit_rules(**kwargs: Any):
    # Keep this local to avoid config -> approved_profile -> research -> strategy -> config imports.
    from bithumb_bot.strategy.exit_rules import create_exit_rules

    return create_exit_rules(**kwargs)


def _rss_mb() -> float | None:
    try:
        import resource

        rss = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except Exception:
        return None
    # Linux reports KiB; macOS reports bytes. AWS Linux is the reference runtime.
    if rss > 10_000_000:
        return round(rss / (1024.0 * 1024.0), 3)
    return round(rss / 1024.0, 3)


def _regime_snapshot_value(snapshot: Any, key: str) -> str:
    if isinstance(snapshot, dict):
        return str(snapshot.get(key) or "unknown")
    return str(getattr(snapshot, key, "unknown") or "unknown")


def _retained_detail_summary(
    accumulator: BacktestAccumulator,
    *,
    retained_regime_snapshot_count: int,
) -> dict[str, object]:
    return {
        "report_detail": accumulator.report_detail,
        "decision_count": accumulator.decision_count,
        "retained_decision_count": accumulator.retained_decision_count,
        "retained_equity_point_count": accumulator.retained_equity_point_count,
        "retained_regime_snapshot_count": int(retained_regime_snapshot_count),
        "decision_hash": canonical_payload_hash(accumulator.decision_hash_material),
        **_behavior_hashes(
            decision_material=accumulator.behavior_hash_material,
            common_decision_material=accumulator.common_behavior_hash_material,
            strategy_decision_material=accumulator.strategy_behavior_hash_material,
            trade_material=accumulator.trade_ledger_hash_material,
            equity_material=accumulator.equity_curve_hash_material,
        ),
    }


def _trade_hash_payload(trade: dict[str, object]) -> dict[str, object]:
    execution = trade.get("execution") if isinstance(trade.get("execution"), dict) else {}
    return {
        "ts": trade.get("ts"),
        "side": trade.get("side"),
        "signal_ts": trade.get("signal_ts"),
        "decision_ts": trade.get("decision_ts"),
        "submit_ts_assumption": trade.get("submit_ts_assumption"),
        "fill_reference_ts": trade.get("fill_reference_ts"),
        "portfolio_effective_ts": trade.get("portfolio_effective_ts"),
        "price": trade.get("price"),
        "reference_price": execution.get("reference_price"),
        "avg_fill_price": execution.get("avg_fill_price"),
        "qty": trade.get("qty"),
        "filled_qty": execution.get("filled_qty"),
        "filled_notional": execution.get("filled_notional"),
        "remaining_qty": execution.get("remaining_qty"),
        "fill_status": execution.get("fill_status"),
        "fee": trade.get("fee"),
        "slippage_bps": execution.get("slippage_bps"),
        "cash": trade.get("cash"),
        "asset_qty": trade.get("asset_qty"),
        "pnl": trade.get("pnl"),
        "net_pnl": trade.get("net_pnl"),
        "closed_trade_pnl": trade.get("closed_trade_pnl"),
        "exit_rule": trade.get("exit_rule"),
        "exit_reason": trade.get("exit_reason"),
        "entry_decision_hash": trade.get("entry_decision_hash"),
        "exit_decision_hash": trade.get("exit_decision_hash"),
        "model_name": execution.get("model_name"),
        "model_version": execution.get("model_version"),
        "model_params_hash": execution.get("model_params_hash"),
    }


def _behavior_hashes(
    *,
    decision_material: list[dict[str, object]],
    common_decision_material: list[dict[str, object]] | None = None,
    strategy_decision_material: list[dict[str, object]] | None = None,
    trade_material: list[dict[str, object]],
    equity_material: list[dict[str, object]],
) -> dict[str, str]:
    decision_hash = canonical_payload_hash(decision_material)
    common_decision_hash = canonical_payload_hash(common_decision_material or [])
    strategy_decision_hash = canonical_payload_hash(strategy_decision_material or [])
    trade_hash = canonical_payload_hash(trade_material)
    equity_hash = canonical_payload_hash(equity_material)
    composite_hash = canonical_payload_hash(
        {
            "decision_behavior_hash": decision_hash,
            "trade_ledger_hash": trade_hash,
            "equity_curve_hash": equity_hash,
        }
    )
    composite_hash_v2 = canonical_payload_hash(
        {
            "common_decision_behavior_hash": common_decision_hash,
            "strategy_behavior_hash": strategy_decision_hash,
            "trade_ledger_hash": trade_hash,
            "equity_curve_hash": equity_hash,
        }
    )
    return {
        "decision_behavior_hash": decision_hash,
        "common_decision_behavior_hash": common_decision_hash,
        "strategy_behavior_hash": strategy_decision_hash,
        "trade_ledger_hash": trade_hash,
        "equity_curve_hash": equity_hash,
        "composite_behavior_hash": composite_hash,
        "composite_behavior_hash_v2": composite_hash_v2,
        "behavior_hash": composite_hash,
    }


def _trace_decision(context: BacktestRunContext, payload: dict[str, object]) -> None:
    sink = context.audit_trace
    if sink is None:
        return
    sink.write_decision(dict(payload))


def _trace_equity_mark(
    context: BacktestRunContext,
    *,
    ts: int,
    equity: float,
    cash: float,
    asset_qty: float,
) -> None:
    sink = context.audit_trace
    if sink is None:
        return
    sink.write_equity(
        {
            "ts": int(ts),
            "equity": float(equity),
            "cash": float(cash),
            "asset_qty": float(asset_qty),
        }
    )


def _trace_execution(context: BacktestRunContext, trade: dict[str, object]) -> None:
    sink = context.audit_trace
    if sink is None:
        return
    sink.write_execution(dict(trade))


def _complete_audit_trace(context: BacktestRunContext, *, status: str) -> dict[str, object] | None:
    sink = context.audit_trace
    if sink is None:
        return None
    return sink.complete(status=status)


def _record_equity_mark(
    *,
    equity_curve: list[EquityPoint],
    ts: int,
    cash: float,
    qty: float,
    mark_price: float,
    peak: float,
    max_drawdown: float,
    retain: bool = True,
) -> tuple[float, float]:
    equity = float(cash) + float(qty) * float(mark_price)
    if retain:
        equity_curve.append(
            EquityPoint(
                ts=int(ts),
                equity=equity,
                cash=float(cash),
                asset_qty=float(qty),
            )
        )
    peak = max(float(peak), equity)
    if peak > 0.0:
        max_drawdown = max(float(max_drawdown), (peak - equity) / peak)
    return peak, max_drawdown


def _fill_applies_to_mark(*, fill: Any, effective_ts: int, mark_boundary_ts: int) -> bool:
    if int(effective_ts) < int(mark_boundary_ts):
        return True
    if int(effective_ts) > int(mark_boundary_ts):
        return False
    return (
        bool(getattr(fill, "allow_same_candle_close_fill", False))
        and str(getattr(fill, "fill_reference_policy", "")) == "candle_close_legacy"
    )


def _apply_pending_fills(
    *,
    pending_fills: list[PendingFill],
    trades: list[dict[str, object]],
    boundary_ts: int,
    cash: float,
    qty: float,
    entry_cost_basis: float,
    entry_regime_snapshot: dict[str, object] | None,
    entry_ts: int | None,
    entry_price: float | None,
    entry_decision_hash: str | None,
    open_trade_path: list[dict[str, float | int]],
    entry_fee: float,
    entry_slippage: float,
    fee_total: float,
    slippage_total: float,
    closed_pnls: list[float],
) -> tuple[
    float,
    float,
    float,
    dict[str, object] | None,
    int | None,
    float | None,
    str | None,
    list[dict[str, float | int]],
    float,
    float,
    float,
    float,
]:
    """Ledger-private pending-fill mutation helper.

    PortfolioLedger is the only authority-facing entry point for portfolio
    state mutation. This helper remains in the common module for compatibility
    with historical imports, but callers must not treat it as an independent
    state authority.
    """
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
            entry_ts = int(pending.fill.signal_ts)
            entry_price = float(pending.fill.avg_fill_price or pending.fill.reference_price)
            entry_decision_hash = str(trade.get("entry_decision_hash") or "") or entry_decision_hash
            open_trade_path = []
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
                entry_ts = None
                entry_price = None
                entry_decision_hash = None
                open_trade_path = []
                entry_fee = 0.0
                entry_slippage = 0.0
    return (
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
    )


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


def _depth_request_fields(
    *,
    dataset: DatasetSnapshot,
    reference: ExecutionReferenceEvent,
    model: ExecutionModel,
    timing_policy: ExecutionTimingPolicy,
) -> dict[str, object]:
    if getattr(model, "name", "") != "depth_walk":
        return {}
    target_ts = reference.fill_reference_ts
    if target_ts is None:
        target_ts = reference.submit_ts_assumption
    snapshot = dataset.first_depth_snapshot_after_or_equal(
        target_ts=int(target_ts),
        max_wait_ms=int(timing_policy.max_quote_wait_ms),
    )
    if snapshot is None:
        return {
            "orderbook_depth_snapshot": None,
            "orderbook_depth_ref": None,
            "depth_available": False,
            "depth_sufficient": False,
            "execution_liquidity_evidence_type": "l2_depth_walk_queue_unaware",
            "execution_realism_limitations": (
                "depth_snapshot_missing_for_depth_walk",
                "queue_position_unavailable",
                "market_impact_model_unavailable",
                "trade_ticks_unavailable",
                "intra_candle_path_reconstruction_unavailable",
            ),
        }
    return {
        "orderbook_depth_snapshot": snapshot,
        "orderbook_depth_ref": snapshot.depth_ref(),
        "depth_snapshot_ts": int(snapshot.ts),
        "depth_snapshot_age_ms": int(snapshot.ts) - int(target_ts),
        "depth_available": True,
        "execution_liquidity_evidence_type": "l2_depth_walk_queue_unaware",
        "execution_realism_limitations": (
            "queue_position_unavailable",
            "market_impact_model_unavailable",
            "trade_ticks_unavailable",
            "intra_candle_path_reconstruction_unavailable",
        ),
    }


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
    dataset_content_hash: str,
    parameter_values: dict[str, Any],
    strategy_name: str,
    strategy_spec: dict[str, Any],
    strategy_spec_hash: str,
    strategy_plugin_contract: dict[str, Any],
    strategy_plugin_contract_hash: str,
    exit_policy: dict[str, Any],
    exit_policy_hash: str,
    fee_rate: float,
    slippage_bps: float,
    timing_policy: ExecutionTimingPolicy,
    portfolio_policy: PortfolioPolicy,
    candle_ts: int,
    decision_ts: int,
    raw_signal: str,
    entry_signal: str,
    exit_signal: str,
    final_signal: str,
    raw_reason: str,
    blocked: bool,
    raw_filter_would_block: bool,
    entry_blocked: bool,
    protective_exit_overrode_entry: bool,
    exit_filter_suppression_prevented: bool,
    blocked_filters: list[str],
    feature_snapshot: dict[str, object] | None,
    regime_snapshot: dict[str, object],
    entry_reason: str,
    market_regime_decision: dict[str, object],
    market_regime_blocked: bool,
    candidate_regime_blocked: bool,
    qty: float,
    sellable_qty: float,
    exit_rule: str = "",
    exit_reason: str = "",
    exit_evaluations: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    from bithumb_bot.research.lot_native_simulation import lot_native_model_from_quantities

    order_rules = {
        "source": "research_execution_model",
        "fee_rate": float(fee_rate),
        "slippage_bps": float(slippage_bps),
        "portfolio_policy_hash": portfolio_policy.policy_hash(),
        "position_sizing": portfolio_policy.position_sizing.as_dict(),
        "sizing": (
            f"cash_fraction_{portfolio_policy.position_sizing.buy_fraction:g}"
            "_or_full_sellable_qty"
        ),
    }
    fee_authority_hash = canonical_payload_hash({"source": "research_manifest", "fee_rate": float(fee_rate)})
    order_rules_hash = canonical_payload_hash(order_rules)
    fee_model_hash = canonical_payload_hash({"fee_rate": float(fee_rate)})
    slippage_model_hash = canonical_payload_hash({"slippage_bps": float(slippage_bps)})
    execution_timing_policy_hash = canonical_payload_hash(timing_policy.as_dict())
    portfolio_policy_hash = portfolio_policy.policy_hash()
    decision_contract_hash = canonical_payload_hash(
        {
            "dataset_content_hash": dataset_content_hash,
            "parameter_values": parameter_values,
            "candle_ts": int(candle_ts),
            "portfolio_policy_hash": portfolio_policy_hash,
            "execution_timing_policy_hash": execution_timing_policy_hash,
            "fee_model_hash": fee_model_hash,
            "slippage_model_hash": slippage_model_hash,
            "strategy_spec_hash": strategy_spec_hash,
            "exit_policy_hash": exit_policy_hash,
        }
    )
    lot_native_authority = lot_native_model_from_quantities(
        qty=float(qty),
        sellable_qty=float(sellable_qty),
    ).authority_snapshot(
        order_rules_hash=order_rules_hash,
        fee_authority_hash=fee_authority_hash,
    )
    flat_no_position = lot_native_authority.state_class == "flat_no_dust_no_position"
    position_state_hash = lot_native_authority.position_state_hash
    if lot_native_authority.unsupported_reason:
        legacy_authority = research_position_authority_snapshot(
            qty=float(qty),
            sellable_qty=float(sellable_qty),
            order_rules_hash=order_rules_hash,
            fee_authority_hash=fee_authority_hash,
            position_state_hash=canonical_payload_hash(
                {
                    "research_position_model": "cash_qty_simulation_v1",
                    "unsupported_reason": "research_model_lacks_lot_native_authority",
                    "qty": float(qty),
                    "sellable_qty": float(sellable_qty),
                }
            ),
        )
        position_state_hash = legacy_authority.position_state_hash
    else:
        legacy_authority = None
    payload = {
        "strategy_name": strategy_name,
        "strategy_spec": strategy_spec,
        "strategy_spec_hash": strategy_spec_hash,
        "strategy_plugin_contract": strategy_plugin_contract,
        "strategy_plugin_contract_hash": strategy_plugin_contract_hash,
        "dataset_content_hash": dataset_content_hash,
        "parameter_values_hash": canonical_payload_hash(parameter_values),
        "exit_policy": exit_policy,
        "exit_policy_hash": exit_policy_hash,
        "market": dataset.market,
        "interval": dataset.interval,
        "signal_timestamp": str(candle_ts),
        "candle_ts": int(candle_ts),
        "through_ts_ms": int(candle_ts),
        "candle_basis": "research_closed_candle",
        "decision_ts": int(decision_ts),
        "raw_signal": raw_signal,
        "entry_signal": entry_signal,
        "exit_signal": exit_signal,
        "final_signal": final_signal,
        "side": final_signal,
        "entry_reason": str(entry_reason),
        "blocked": bool(blocked),
        "raw_filter_would_block": bool(raw_filter_would_block),
        "entry_blocked": bool(entry_blocked),
        "protective_exit_overrode_entry": bool(protective_exit_overrode_entry),
        # Legacy compatibility alias: for SELL this means filters would have
        # blocked the raw signal if entry filters governed exits.
        "entry_filter_blocked": bool(raw_filter_would_block),
        "exit_filter_suppression_prevented": bool(exit_filter_suppression_prevented),
        "entry_blocked_filters": tuple(blocked_filters),
        "block_reason": str(entry_reason) if blocked else "",
        "blocked_filters": tuple(blocked_filters),
        "sellable_qty": float(sellable_qty),
        "fee_authority_hash": fee_authority_hash,
        "fee_model_hash": fee_model_hash,
        "slippage_model_hash": slippage_model_hash,
        "order_rules_hash": order_rules_hash,
        "market_regime": str(regime_snapshot.get("composite_regime") or ""),
        "current_market_regime_snapshot": regime_snapshot,
        "current_regime": str(market_regime_decision.get("current_regime") or regime_snapshot.get("composite_regime") or ""),
        "regime_decision": market_regime_decision.get("regime_decision") or "not_configured",
        "regime_block_reason": market_regime_decision.get("regime_block_reason") or "",
        "market_regime_blocked": bool(market_regime_blocked),
        "candidate_regime_blocked": bool(candidate_regime_blocked),
        "position_state_hash": position_state_hash,
        "entry_allowed": bool(lot_native_authority.entry_allowed),
        "exit_allowed": bool(lot_native_authority.exit_allowed),
        "dust_state": "flat" if flat_no_position else (
            "research_not_modeled" if lot_native_authority.unsupported_reason else "no_dust"
        ),
        "effective_flat": bool(lot_native_authority.entry_allowed),
        "normalized_exposure_active": bool(lot_native_authority.open_lot_count > 0),
        "exit_rule": str(exit_rule or ""),
        "exit_reason": str(exit_reason or ""),
        "exit_evaluations_hash": canonical_payload_hash(
            {
                "raw_signal": raw_signal,
                "final_signal": final_signal,
                "position_qty": float(qty),
                "exit_rule": str(exit_rule or ""),
                "exit_reason": str(exit_reason or ""),
                "exit_evaluations": exit_evaluations or [],
                "exit_policy_hash": exit_policy_hash,
            }
        ),
        "exit_evaluations": exit_evaluations or [],
        "portfolio_policy_hash": portfolio_policy_hash,
        "execution_timing_policy_hash": execution_timing_policy_hash,
        "decision_contract_hash": decision_contract_hash,
        "replay_fingerprint_hash": decision_contract_hash,
    }
    payload["position_authority"] = (
        legacy_authority.as_dict() if legacy_authority is not None else lot_native_authority.as_dict()
    )
    return payload


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
        requested_notional=requested_notional,
        filled_notional=0.0,
        depth_available=False,
        depth_sufficient=False,
        queue_position_mode="unavailable",
        market_impact_mode="unavailable",
        execution_liquidity_evidence_type="top_of_book_quote_only" if reference.quote_ts is not None else "candle_only",
        execution_realism_limitations=(
            "full_orderbook_depth_unavailable",
            "queue_position_unavailable",
            "market_impact_model_unavailable",
        ),
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
    pending_fills: list[PendingFill],
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


def _diagnostic_count_defaults(payload: dict[str, object]) -> dict[str, int]:
    defaults = payload.get("strategy_diagnostic_count_defaults")
    if not isinstance(defaults, dict):
        return {}
    return {
        str(key): int(value)
        for key, value in defaults.items()
        if _diagnostic_key_is_public(str(key))
    }


def _diagnostic_count_increments(payload: dict[str, object]) -> dict[str, int]:
    counts = payload.get("strategy_diagnostic_counts")
    if not isinstance(counts, dict):
        return {}
    increments: dict[str, int] = {}
    for key, value in counts.items():
        normalized = str(key)
        if not _diagnostic_key_is_public(normalized):
            continue
        increments[normalized] = increments.get(normalized, 0) + int(value)
    return increments


def _diagnostic_key_is_public(key: str) -> bool:
    return bool(key) and not key.startswith("_")


def _strategy_diagnostics_from_trades(
    *,
    namespace: str = "sma_with_filter",
    trades: list[dict[str, object]],
) -> dict[str, object]:
    closed = [
        trade
        for trade in trades
        if isinstance(trade, dict)
        and bool(trade.get("is_portfolio_applied_trade"))
        and str(trade.get("side") or "").upper() == "SELL"
    ]
    exit_reason_distribution: dict[str, int] = {}
    mae_pct_by_trade: list[float] = []
    mfe_pct_by_trade: list[float] = []
    loss_holding_minutes: list[float] = []
    for trade in closed:
        reason = str(trade.get("exit_rule") or trade.get("exit_reason") or "unknown")
        exit_reason_distribution[reason] = exit_reason_distribution.get(reason, 0) + 1
        if trade.get("mae_pct") is not None:
            mae_pct_by_trade.append(float(trade.get("mae_pct") or 0.0))
        if trade.get("mfe_pct") is not None:
            mfe_pct_by_trade.append(float(trade.get("mfe_pct") or 0.0))
        pnl = trade.get("net_pnl") if trade.get("net_pnl") is not None else trade.get("closed_trade_pnl")
        if pnl is not None and float(pnl) < 0.0 and trade.get("holding_minutes") is not None:
            loss_holding_minutes.append(float(trade.get("holding_minutes") or 0.0))
    payload = {
        "schema_version": 1,
        "exit_reason_distribution": dict(sorted(exit_reason_distribution.items())),
        "mae_pct_by_trade": mae_pct_by_trade,
        "mfe_pct_by_trade": mfe_pct_by_trade,
        "p95_mae_pct": _percentile(mae_pct_by_trade, 0.95),
        "p05_mae_pct": _percentile(mae_pct_by_trade, 0.05),
        "p95_adverse_excursion_abs_pct": _percentile(
            [abs(value) for value in mae_pct_by_trade],
            0.95,
        ),
        "worst_trade_mae_pct": min(mae_pct_by_trade) if mae_pct_by_trade else None,
        "avg_loss_holding_minutes": (
            sum(loss_holding_minutes) / len(loss_holding_minutes)
            if loss_holding_minutes
            else None
        ),
    }
    strategy_specific = dict(payload)
    payload["strategy_diagnostics_namespace"] = str(namespace)
    payload["strategy_specific_diagnostics"] = {str(namespace): strategy_specific}
    return payload


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * float(percentile)))))
    return ordered[index]


def empty_execution_event_summary() -> dict[str, object]:
    return execution_event_summary(())


def _empty_metrics_v2(*, starting_cash: float | None = None, initial_position_qty: float = 0.0) -> MetricContractV2:
    cash = float(starting_cash if starting_cash is not None else legacy_research_portfolio_policy().starting_cash_krw)
    return build_metrics_v2(
        starting_cash=cash,
        final_cash=cash,
        final_asset_qty=float(initial_position_qty),
        final_mark_price=0.0,
        equity_curve=(),
        position_intervals=(),
        closed_trades=(),
        execution_records=(),
    )


def _metrics_v2_ledgers_from_trades(
    *,
    trades: list[dict[str, object]],
) -> tuple[tuple[PositionInterval, ...], tuple[ClosedTradeRecord, ...], tuple[ExecutionRecord, ...], float]:
    execution_records = tuple(_execution_record_from_trade(trade) for trade in trades if isinstance(trade, dict))
    applied = sorted(
        [trade for trade in trades if isinstance(trade, dict) and bool(trade.get("is_portfolio_applied_trade"))],
        key=lambda trade: (
            int(trade.get("portfolio_effective_ts") or trade.get("fill_ts") or trade.get("ts") or 0),
            str(trade.get("side") or ""),
        ),
    )
    intervals: list[PositionInterval] = []
    closed: list[ClosedTradeRecord] = []
    open_ts: int | None = None
    open_qty = 0.0
    open_cost_basis = 0.0
    for trade in applied:
        side = str(trade.get("side") or "").upper()
        ts = int(trade.get("portfolio_effective_ts") or trade.get("fill_ts") or trade.get("ts") or 0)
        qty = float(trade.get("qty") or 0.0)
        price = float(trade.get("price") or 0.0)
        fee = float(trade.get("fee") or 0.0)
        if side == "BUY" and qty > 0.0:
            if open_qty <= 1e-12:
                open_ts = ts
                open_cost_basis = 0.0
            open_qty += qty
            open_cost_basis += qty * price + fee
        elif side == "SELL" and qty > 0.0:
            basis_fraction = min(1.0, qty / open_qty) if open_qty > 1e-12 else 0.0
            allocated_basis = open_cost_basis * basis_fraction
            pnl = trade.get("net_pnl") if trade.get("net_pnl") is not None else trade.get("closed_trade_pnl")
            if pnl is not None:
                closed.append(
                    ClosedTradeRecord(
                        entry_ts=open_ts,
                        exit_ts=ts,
                        entry_notional=allocated_basis if allocated_basis > 0.0 else None,
                        net_pnl=float(pnl),
                        return_pct=(float(pnl) / allocated_basis * 100.0) if allocated_basis > 0.0 else None,
                        holding_minutes=(
                            float(trade.get("holding_minutes"))
                            if trade.get("holding_minutes") is not None
                            else None
                        ),
                        entry_price=(
                            float(trade.get("entry_price"))
                            if trade.get("entry_price") is not None
                            else None
                        ),
                        exit_price=(
                            float(trade.get("exit_price"))
                            if trade.get("exit_price") is not None
                            else None
                        ),
                        entry_regime=(
                            str(trade.get("entry_regime"))
                            if trade.get("entry_regime") is not None
                            else None
                        ),
                        exit_regime=(
                            str(trade.get("exit_regime"))
                            if trade.get("exit_regime") is not None
                            else None
                        ),
                        exit_rule=(
                            str(trade.get("exit_rule")) if trade.get("exit_rule") is not None else None
                        ),
                        exit_reason=(
                            str(trade.get("exit_reason")) if trade.get("exit_reason") is not None else None
                        ),
                        mae=float(trade.get("mae")) if trade.get("mae") is not None else None,
                        mfe=float(trade.get("mfe")) if trade.get("mfe") is not None else None,
                        mae_pct=float(trade.get("mae_pct")) if trade.get("mae_pct") is not None else None,
                        mfe_pct=float(trade.get("mfe_pct")) if trade.get("mfe_pct") is not None else None,
                        bars_to_mae=(
                            int(trade.get("bars_to_mae"))
                            if trade.get("bars_to_mae") is not None
                            else None
                        ),
                        bars_to_mfe=(
                            int(trade.get("bars_to_mfe"))
                            if trade.get("bars_to_mfe") is not None
                            else None
                        ),
                        unrealized_pnl_path_summary=(
                            dict(trade.get("unrealized_pnl_path_summary"))
                            if isinstance(trade.get("unrealized_pnl_path_summary"), dict)
                            else None
                        ),
                        entry_decision_hash=(
                            str(trade.get("entry_decision_hash"))
                            if trade.get("entry_decision_hash") is not None
                            else None
                        ),
                        exit_decision_hash=(
                            str(trade.get("exit_decision_hash"))
                            if trade.get("exit_decision_hash") is not None
                            else None
                        ),
                        fee_total=float(trade.get("fee_total") or fee),
                        slippage_total=float(trade.get("slippage_total") or 0.0),
                    )
                )
            open_qty = max(0.0, open_qty - qty)
            open_cost_basis = max(0.0, open_cost_basis - allocated_basis)
            if open_qty <= 1e-12:
                if open_ts is not None:
                    intervals.append(PositionInterval(open_ts=open_ts, close_ts=ts))
                open_ts = None
                open_cost_basis = 0.0
    if open_ts is not None:
        intervals.append(PositionInterval(open_ts=open_ts, close_ts=None))
    return tuple(intervals), tuple(closed), execution_records, open_cost_basis


def _execution_record_from_trade(trade: dict[str, object]) -> ExecutionRecord:
    execution = trade.get("execution") if isinstance(trade.get("execution"), dict) else {}
    assert isinstance(execution, dict)
    return ExecutionRecord(
        side=str(trade.get("side") or execution.get("side") or ""),
        status=str(execution.get("fill_status") or ""),
        filled_qty=float(execution.get("filled_qty") or trade.get("qty") or 0.0),
        price=(
            float(execution.get("avg_fill_price"))
            if execution.get("avg_fill_price") is not None
            else (float(trade.get("price")) if trade.get("price") is not None else None)
        ),
        fee=float(execution.get("fee") or trade.get("fee") or 0.0),
        slippage=float(_trade_execution_slippage(trade)),
        quote_age_ms=int(execution["quote_age_ms"]) if execution.get("quote_age_ms") is not None else None,
    )


def _trade_execution_slippage(trade: dict[str, object]) -> float:
    execution = trade.get("execution") if isinstance(trade.get("execution"), dict) else {}
    assert isinstance(execution, dict)
    status = str(execution.get("fill_status") or "")
    if status not in {"filled", "partial"}:
        return 0.0
    side = str(execution.get("side") or trade.get("side") or "").upper()
    qty = float(execution.get("filled_qty") or trade.get("qty") or 0.0)
    avg_price = execution.get("avg_fill_price")
    ref_price = execution.get("reference_price")
    if avg_price is None or ref_price is None or qty <= 0.0:
        return 0.0
    if side == "BUY":
        return max(0.0, (float(avg_price) - float(ref_price)) * qty)
    if side == "SELL":
        return max(0.0, (float(ref_price) - float(avg_price)) * qty)
    return 0.0


def _closed_trade_diagnostics(
    *,
    entry_ts: int | None,
    exit_ts: int,
    entry_price: float | None,
    exit_price: float,
    entry_regime_snapshot: dict[str, object] | None,
    exit_regime_snapshot: dict[str, object] | None,
    exit_rule: str,
    exit_reason: str,
    path: list[dict[str, float | int]],
    entry_decision_hash: str | None,
    exit_decision_hash: str,
) -> dict[str, object]:
    points = list(path)
    mae_point = min(points, key=lambda item: float(item.get("unrealized_pnl", 0.0)), default=None)
    mfe_point = max(points, key=lambda item: float(item.get("unrealized_pnl", 0.0)), default=None)
    entry_ts_int = int(entry_ts) if entry_ts is not None else None
    holding_minutes = (
        max(0.0, (int(exit_ts) - int(entry_ts_int)) / 60_000.0)
        if entry_ts_int is not None
        else None
    )
    return {
        "entry_ts": entry_ts_int,
        "exit_ts": int(exit_ts),
        "holding_minutes": holding_minutes,
        "entry_price": float(entry_price) if entry_price is not None else None,
        "exit_price": float(exit_price),
        "entry_regime": _regime_snapshot_value(entry_regime_snapshot, "composite_regime"),
        "exit_regime": _regime_snapshot_value(exit_regime_snapshot, "composite_regime"),
        "exit_rule": str(exit_rule or "unknown"),
        "exit_reason": str(exit_reason or "unknown"),
        "mae": float(mae_point.get("unrealized_pnl", 0.0)) if mae_point else 0.0,
        "mfe": float(mfe_point.get("unrealized_pnl", 0.0)) if mfe_point else 0.0,
        "mae_pct": float(mae_point.get("unrealized_pnl_pct", 0.0)) if mae_point else 0.0,
        "mfe_pct": float(mfe_point.get("unrealized_pnl_pct", 0.0)) if mfe_point else 0.0,
        "bars_to_mae": points.index(mae_point) if mae_point in points else None,
        "bars_to_mfe": points.index(mfe_point) if mfe_point in points else None,
        "unrealized_pnl_path_summary": {
            "point_count": len(points),
            "first": points[0] if points else None,
            "last": points[-1] if points else None,
            "mae_point": mae_point,
            "mfe_point": mfe_point,
        },
        "entry_decision_hash": str(entry_decision_hash or ""),
        "exit_decision_hash": str(exit_decision_hash or ""),
    }


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
    profit_factor_unbounded = bool(wins and not losses)
    profit_factor = (sum(wins) / abs(sum(losses))) if losses else None
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
        profit_factor_unbounded=profit_factor_unbounded,
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


closed_trade_diagnostics = _closed_trade_diagnostics
complete_audit_trace = _complete_audit_trace
create_exit_rules = _create_exit_rules
depth_request_fields = _depth_request_fields
apply_pending_fills = _apply_pending_fills
empty_metrics = _empty_metrics
empty_metrics_v2 = _empty_metrics_v2
execution_reference_warnings = _execution_reference_warnings
failed_fill = _failed_fill
fill_applies_to_mark = _fill_applies_to_mark
fill_effective_ts = _fill_effective_ts
mark_pending_fills_at_end = _mark_pending_fills_at_end
metrics = _metrics
metrics_v2_ledgers_from_trades = _metrics_v2_ledgers_from_trades
model_latency_ms = _model_latency_ms
pending_trade_from_fill = _pending_trade_from_fill
record_equity_mark = _record_equity_mark
research_decision_payload = _research_decision_payload
retained_detail_summary = _retained_detail_summary
timing_request_fields = _timing_request_fields
trace_decision = _trace_decision
trace_equity_mark = _trace_equity_mark
trace_execution = _trace_execution
trade_from_fill = _trade_from_fill
trade_hash_payload = _trade_hash_payload
