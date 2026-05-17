from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bithumb_bot.orderbook_depth_store import OrderbookDepthLevel, OrderbookDepthSnapshot

from .base import ExecutionFill, ExecutionRequest, model_params_hash


@dataclass
class DepthWalkExecutionModel:
    fee_rate: float
    depth_snapshot: OrderbookDepthSnapshot

    name: str = "depth_walk"
    version: str = "research_depth_walk_v1"

    def params_payload(self) -> dict[str, Any]:
        return {
            "type": self.name,
            "version": self.version,
            "fee_rate": float(self.fee_rate),
            "depth_ref": self.depth_snapshot.depth_ref(),
            "queue_position_mode": "unavailable",
            "market_impact_mode": "unavailable",
        }

    def simulate(self, request: ExecutionRequest) -> ExecutionFill:
        side = str(request.side).upper()
        if side == "BUY":
            requested_notional = float(request.requested_notional or 0.0)
            requested_qty = (
                float(request.requested_qty)
                if request.requested_qty is not None
                else (
                    requested_notional / float(request.reference_price)
                    if float(request.reference_price) > 0.0
                    else 0.0
                )
            )
            filled_qty, filled_notional, levels_consumed = _walk_buy(
                self.depth_snapshot.asks,
                requested_notional=requested_notional,
                requested_qty=requested_qty,
            )
        elif side == "SELL":
            requested_qty = float(request.requested_qty or 0.0)
            filled_qty, filled_notional, levels_consumed = _walk_sell(
                self.depth_snapshot.bids,
                requested_qty=requested_qty,
            )
            requested_notional = requested_qty * float(request.reference_price)
        else:
            raise ValueError(f"unsupported execution side: {request.side}")

        avg_fill_price = filled_notional / filled_qty if filled_qty > 0.0 else None
        remaining_qty = max(0.0, requested_qty - filled_qty)
        depth_sufficient = filled_qty >= requested_qty and requested_qty > 0.0
        if filled_qty <= 0.0:
            fill_status = "unfilled"
            reason = "depth_unavailable_for_requested_side"
        elif depth_sufficient:
            fill_status = "filled"
            reason = None
        else:
            fill_status = "partial"
            reason = "insufficient_depth_liquidity"

        fee = filled_notional * float(self.fee_rate)
        slippage_bps = 0.0
        if avg_fill_price is not None and float(request.reference_price) > 0.0:
            if side == "BUY":
                slippage_bps = ((avg_fill_price - float(request.reference_price)) / float(request.reference_price)) * 10_000.0
            else:
                slippage_bps = ((float(request.reference_price) - avg_fill_price) / float(request.reference_price)) * 10_000.0

        depth_age_ms = (
            int(self.depth_snapshot.ts) - int(request.fill_reference_ts)
            if request.fill_reference_ts is not None
            else (
                int(self.depth_snapshot.ts) - int(request.decision_ts)
            )
        )
        return ExecutionFill(
            signal_ts=int(request.signal_ts),
            decision_ts=int(request.decision_ts),
            submit_ts_assumption=int(request.submit_ts_assumption if request.submit_ts_assumption is not None else request.decision_ts),
            side=side,
            order_type=request.order_type,
            reference_price=float(request.reference_price),
            fill_reference_ts=request.fill_reference_ts,
            fill_reference_price=request.fill_reference_price,
            fill_reference_source=request.fill_reference_source,
            signal_candle_start_ts=request.signal_candle_start_ts,
            signal_candle_close_ts=request.signal_candle_close_ts,
            signal_reference_price=request.signal_reference_price,
            signal_reference_source=request.signal_reference_source,
            quote_ts=request.quote_ts,
            quote_age_ms=request.quote_age_ms,
            quote_source=request.quote_source,
            requested_qty=float(requested_qty),
            filled_qty=float(filled_qty),
            remaining_qty=float(remaining_qty),
            avg_fill_price=avg_fill_price,
            fee=float(fee),
            slippage_bps=float(slippage_bps),
            latency_ms=0,
            fill_status=fill_status,
            model_name=self.name,
            model_version=self.version,
            model_params_hash=model_params_hash(self.params_payload()),
            best_bid=request.best_bid,
            best_ask=request.best_ask,
            spread_bps=request.spread_bps,
            orderbook_depth_ref=self.depth_snapshot.depth_ref(),
            requested_notional=float(requested_notional),
            filled_notional=float(filled_notional),
            depth_snapshot_ts=int(self.depth_snapshot.ts),
            depth_snapshot_age_ms=int(depth_age_ms),
            depth_levels_consumed=int(levels_consumed),
            depth_available=True,
            depth_sufficient=bool(depth_sufficient),
            queue_position_mode="unavailable",
            market_impact_mode="unavailable",
            execution_liquidity_evidence_type="l2_depth_walk_queue_unaware",
            execution_realism_limitations=(
                "queue_position_unavailable",
                "market_impact_model_unavailable",
                "trade_ticks_unavailable",
                "intra_candle_path_reconstruction_unavailable",
            ) if reason is None else (
                reason,
                "queue_position_unavailable",
                "market_impact_model_unavailable",
                "trade_ticks_unavailable",
                "intra_candle_path_reconstruction_unavailable",
            ),
            execution_reality_level="latency_adjusted_top_of_book",
            allow_same_candle_close_fill=request.allow_same_candle_close_fill,
            quote_selection=request.quote_selection,
            fill_reference_policy=request.fill_reference_policy,
            top_of_book_source=request.top_of_book_source or request.quote_source,
            top_of_book_is_full_depth=False,
            execution_reference_failure_reason=reason or request.execution_reference_failure_reason,
            latency_applied_to_reference=request.latency_applied_to_reference,
            latency_applied_to_submit_ts=request.latency_applied_to_submit_ts,
            latency_applied_to_fill_reference=request.latency_applied_to_fill_reference,
            latency_reference_policy_warning=request.latency_reference_policy_warning,
            feature_snapshot=request.feature_snapshot,
            regime_snapshot=request.regime_snapshot,
            intra_candle_policy="depth_walk_l2_no_queue_no_impact",
        )
def _walk_buy(
    levels: tuple[OrderbookDepthLevel, ...],
    *,
    requested_notional: float,
    requested_qty: float,
) -> tuple[float, float, int]:
    remaining_qty = max(0.0, float(requested_qty))
    remaining_notional = max(0.0, float(requested_notional))
    filled_qty = 0.0
    filled_notional = 0.0
    levels_consumed = 0
    for level in levels:
        if remaining_qty <= 0.0 or remaining_notional <= 0.0:
            break
        take_qty = min(level.size, remaining_qty, remaining_notional / level.price)
        if take_qty <= 0.0:
            break
        filled_qty += take_qty
        notional = take_qty * level.price
        filled_notional += notional
        remaining_qty -= take_qty
        remaining_notional -= notional
        levels_consumed += 1
    return filled_qty, filled_notional, levels_consumed


def _walk_sell(
    levels: tuple[OrderbookDepthLevel, ...],
    *,
    requested_qty: float,
) -> tuple[float, float, int]:
    remaining_qty = max(0.0, float(requested_qty))
    filled_qty = 0.0
    filled_notional = 0.0
    levels_consumed = 0
    for level in levels:
        if remaining_qty <= 0.0:
            break
        take_qty = min(level.size, remaining_qty)
        if take_qty <= 0.0:
            break
        filled_qty += take_qty
        filled_notional += take_qty * level.price
        remaining_qty -= take_qty
        levels_consumed += 1
    return filled_qty, filled_notional, levels_consumed
