from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .base import ExecutionFill, ExecutionRequest, model_params_hash


@dataclass
class FixedBpsExecutionModel:
    fee_rate: float
    slippage_bps: float

    name: str = "fixed_bps"
    version: str = "research_fixed_bps_v1"

    def params_payload(self) -> dict[str, Any]:
        return {
            "type": self.name,
            "version": self.version,
            "fee_rate": float(self.fee_rate),
            "slippage_bps": float(self.slippage_bps),
        }

    def simulate(self, request: ExecutionRequest) -> ExecutionFill:
        side = str(request.side).upper()
        slip = float(self.slippage_bps) / 10_000.0
        if side == "BUY":
            avg_fill_price = request.reference_price * (1.0 + slip)
            requested_qty = (
                (float(request.requested_notional or 0.0) * (1.0 - float(self.fee_rate))) / avg_fill_price
                if avg_fill_price > 0.0
                else 0.0
            )
            filled_qty = requested_qty
            fee = float(request.requested_notional or 0.0) * float(self.fee_rate)
        elif side == "SELL":
            avg_fill_price = request.reference_price * (1.0 - slip)
            requested_qty = float(request.requested_qty or 0.0)
            filled_qty = requested_qty
            fee = filled_qty * avg_fill_price * float(self.fee_rate)
        else:
            raise ValueError(f"unsupported execution side: {request.side}")
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
            remaining_qty=max(0.0, float(requested_qty) - float(filled_qty)),
            avg_fill_price=float(avg_fill_price),
            fee=float(fee),
            slippage_bps=float(self.slippage_bps),
            latency_ms=0,
            fill_status="filled",
            model_name=self.name,
            model_version=self.version,
            model_params_hash=model_params_hash(self.params_payload()),
            best_bid=request.best_bid,
            best_ask=request.best_ask,
            spread_bps=request.spread_bps,
            requested_notional=request.requested_notional,
            filled_notional=(float(filled_qty) * float(avg_fill_price)),
            depth_snapshot_ts=request.depth_snapshot_ts,
            depth_snapshot_age_ms=request.depth_snapshot_age_ms,
            depth_levels_consumed=request.depth_levels_consumed,
            depth_available=bool(request.depth_available),
            depth_sufficient=request.depth_sufficient,
            queue_position_mode=request.queue_position_mode,
            market_impact_mode=request.market_impact_mode,
            execution_liquidity_evidence_type=request.execution_liquidity_evidence_type,
            execution_realism_limitations=request.execution_realism_limitations,
            execution_reality_level=request.execution_reality_level,
            allow_same_candle_close_fill=request.allow_same_candle_close_fill,
            quote_selection=request.quote_selection,
            fill_reference_policy=request.fill_reference_policy,
            top_of_book_source=request.top_of_book_source or request.quote_source,
            top_of_book_is_full_depth=request.top_of_book_is_full_depth,
            execution_reference_failure_reason=request.execution_reference_failure_reason,
            latency_applied_to_reference=request.latency_applied_to_reference,
            latency_applied_to_submit_ts=request.latency_applied_to_submit_ts,
            latency_applied_to_fill_reference=request.latency_applied_to_fill_reference,
            latency_reference_policy_warning=request.latency_reference_policy_warning,
            feature_snapshot=request.feature_snapshot,
            regime_snapshot=request.regime_snapshot,
            intra_candle_policy=request.intra_candle_policy,
            base_seed=None,
            derived_seed_hash=None,
            seed_derivation_inputs=None,
        )
