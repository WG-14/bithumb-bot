from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bithumb_bot.research.hashing import sha256_hex, sha256_prefixed

from .base import ExecutionFill, ExecutionRequest, model_params_hash


@dataclass
class StressExecutionModel:
    fee_rate: float
    slippage_bps: float
    latency_ms: int = 0
    partial_fill_rate: float = 0.0
    order_failure_rate: float = 0.0
    market_order_extra_cost_bps: float = 0.0
    seed: int | None = None
    partial_fill_fraction: float = 0.5
    seed_derivation_inputs: dict[str, Any] | None = None

    name: str = "stress"
    version: str = "research_stress_v1"

    def params_payload(self) -> dict[str, Any]:
        return {
            "type": self.name,
            "version": self.version,
            "fee_rate": float(self.fee_rate),
            "slippage_bps": float(self.slippage_bps),
            "latency_ms": int(self.latency_ms),
            "partial_fill_rate": float(self.partial_fill_rate),
            "order_failure_rate": float(self.order_failure_rate),
            "market_order_extra_cost_bps": float(self.market_order_extra_cost_bps),
            "seed": self.seed,
            "partial_fill_fraction": float(self.partial_fill_fraction),
            "seed_derivation_inputs": self.seed_derivation_inputs,
        }

    def simulate(self, request: ExecutionRequest) -> ExecutionFill:
        side = str(request.side).upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"unsupported execution side: {request.side}")
        total_slippage_bps = float(self.slippage_bps)
        if str(request.order_type).lower() == "market":
            total_slippage_bps += float(self.market_order_extra_cost_bps)
        slip = total_slippage_bps / 10_000.0
        if side == "BUY":
            avg_fill_price = request.reference_price * (1.0 + slip)
            requested_qty = (
                (float(request.requested_notional or 0.0) * (1.0 - float(self.fee_rate))) / avg_fill_price
                if avg_fill_price > 0.0
                else 0.0
            )
        else:
            avg_fill_price = request.reference_price * (1.0 - slip)
            requested_qty = float(request.requested_qty or 0.0)

        seed_inputs = {
            "base_seed": self.seed,
            "model_params_hash": model_params_hash(self.params_payload()),
            "request": {
                "signal_ts": int(request.signal_ts),
                "decision_ts": int(request.decision_ts),
                "side": side,
                "order_type": request.order_type,
                "reference_price": float(request.reference_price),
            },
            **(self.seed_derivation_inputs or {}),
        }
        derived_seed_hash = sha256_prefixed(seed_inputs)
        rng = _DeterministicUnitRng(derived_seed_hash)

        fill_status = "filled"
        fill_ratio = 1.0
        if rng.unit_float("order_failure") < float(self.order_failure_rate):
            fill_status = "failed"
            fill_ratio = 0.0
        elif rng.unit_float("partial_fill") < float(self.partial_fill_rate):
            fill_status = "partial"
            fill_ratio = min(max(float(self.partial_fill_fraction), 0.0), 1.0)

        filled_qty = requested_qty * fill_ratio
        if side == "BUY":
            fee = float(request.requested_notional or 0.0) * float(self.fee_rate) * fill_ratio
        else:
            fee = filled_qty * avg_fill_price * float(self.fee_rate)
        return ExecutionFill(
            signal_ts=int(request.signal_ts),
            decision_ts=int(request.decision_ts),
            submit_ts_assumption=int(
                request.submit_ts_assumption
                if request.submit_ts_assumption is not None
                else int(request.decision_ts) + int(self.latency_ms)
            ),
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
            avg_fill_price=(float(avg_fill_price) if fill_ratio > 0.0 else None),
            fee=float(fee),
            slippage_bps=float(total_slippage_bps),
            latency_ms=int(self.latency_ms),
            fill_status=fill_status,
            model_name=self.name,
            model_version=self.version,
            model_params_hash=model_params_hash(self.params_payload()),
            best_bid=request.best_bid,
            best_ask=request.best_ask,
            spread_bps=request.spread_bps,
            execution_reality_level=request.execution_reality_level,
            allow_same_candle_close_fill=request.allow_same_candle_close_fill,
            quote_selection=request.quote_selection,
            fill_reference_policy=request.fill_reference_policy,
            top_of_book_source=request.top_of_book_source or request.quote_source,
            top_of_book_is_full_depth=request.top_of_book_is_full_depth,
            execution_reference_failure_reason=request.execution_reference_failure_reason,
            feature_snapshot=request.feature_snapshot,
            regime_snapshot=request.regime_snapshot,
            intra_candle_policy=request.intra_candle_policy,
            base_seed=self.seed,
            derived_seed_hash=derived_seed_hash,
            seed_derivation_inputs=seed_inputs,
        )


class _DeterministicUnitRng:
    def __init__(self, seed_hash: str) -> None:
        self._seed_hash = seed_hash

    def unit_float(self, stream: str) -> float:
        digest = sha256_hex({"seed_hash": self._seed_hash, "stream": stream})
        return int(digest[:16], 16) / float(16**16)
