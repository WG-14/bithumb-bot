from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from bithumb_bot.research.hashing import sha256_prefixed


@dataclass(frozen=True)
class ExecutionRequest:
    signal_ts: int
    decision_ts: int
    side: str
    reference_price: float
    fee_rate: float
    order_type: str = "market"
    requested_qty: float | None = None
    requested_notional: float | None = None
    submit_ts_assumption: int | None = None
    fill_reference_ts: int | None = None
    fill_reference_price: float | None = None
    fill_reference_source: str | None = None
    signal_candle_start_ts: int | None = None
    signal_candle_close_ts: int | None = None
    signal_reference_price: float | None = None
    signal_reference_source: str | None = None
    quote_ts: int | None = None
    quote_age_ms: int | None = None
    quote_source: str | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    spread_bps: float | None = None
    execution_reality_level: str | None = None
    allow_same_candle_close_fill: bool | None = None
    quote_selection: str | None = None
    fill_reference_policy: str | None = None
    top_of_book_source: str | None = None
    top_of_book_is_full_depth: bool | None = None
    depth_snapshot_ts: int | None = None
    depth_snapshot_age_ms: int | None = None
    depth_levels_consumed: int | None = None
    depth_available: bool = False
    depth_sufficient: bool | None = None
    queue_position_mode: str = "unavailable"
    market_impact_mode: str = "unavailable"
    execution_liquidity_evidence_type: str = "top_of_book_quote_only"
    execution_realism_limitations: tuple[str, ...] = (
        "full_orderbook_depth_unavailable",
        "queue_position_unavailable",
        "market_impact_model_unavailable",
    )
    execution_reference_failure_reason: str | None = None
    latency_applied_to_reference: bool | None = None
    latency_applied_to_submit_ts: bool | None = None
    latency_applied_to_fill_reference: bool | None = None
    latency_reference_policy_warning: str | None = None
    feature_snapshot: dict[str, Any] | None = None
    regime_snapshot: dict[str, Any] | None = None
    intra_candle_policy: str = "close_price_only_no_intracandle_path"


@dataclass(frozen=True)
class ExecutionFill:
    signal_ts: int
    decision_ts: int
    submit_ts_assumption: int
    side: str
    order_type: str
    reference_price: float
    fill_reference_ts: int | None = None
    fill_reference_price: float | None = None
    fill_reference_source: str | None = None
    signal_candle_start_ts: int | None = None
    signal_candle_close_ts: int | None = None
    signal_reference_price: float | None = None
    signal_reference_source: str | None = None
    quote_ts: int | None = None
    quote_age_ms: int | None = None
    quote_source: str | None = None
    requested_qty: float = 0.0
    filled_qty: float = 0.0
    remaining_qty: float = 0.0
    avg_fill_price: float | None = None
    fee: float = 0.0
    slippage_bps: float = 0.0
    latency_ms: int = 0
    fill_status: str = "filled"
    model_name: str = ""
    model_version: str = ""
    model_params_hash: str = ""
    best_bid: float | None = None
    best_ask: float | None = None
    spread_bps: float | None = None
    orderbook_depth_ref: str | None = None
    requested_notional: float | None = None
    filled_notional: float | None = None
    depth_snapshot_ts: int | None = None
    depth_snapshot_age_ms: int | None = None
    depth_levels_consumed: int | None = None
    depth_available: bool = False
    depth_sufficient: bool | None = None
    queue_position_mode: str = "unavailable"
    market_impact_mode: str = "unavailable"
    execution_liquidity_evidence_type: str = "top_of_book_quote_only"
    execution_realism_limitations: tuple[str, ...] = (
        "full_orderbook_depth_unavailable",
        "queue_position_unavailable",
        "market_impact_model_unavailable",
    )
    execution_reality_level: str | None = None
    allow_same_candle_close_fill: bool | None = None
    quote_selection: str | None = None
    fill_reference_policy: str | None = None
    top_of_book_source: str | None = None
    top_of_book_is_full_depth: bool | None = None
    execution_reference_failure_reason: str | None = None
    latency_applied_to_reference: bool | None = None
    latency_applied_to_submit_ts: bool | None = None
    latency_applied_to_fill_reference: bool | None = None
    latency_reference_policy_warning: str | None = None
    feature_snapshot: dict[str, Any] | None = None
    regime_snapshot: dict[str, Any] | None = None
    intra_candle_policy: str = "close_price_only_no_intracandle_path"
    base_seed: int | None = None
    derived_seed_hash: str | None = None
    seed_derivation_inputs: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "signal_ts": self.signal_ts,
            "decision_ts": self.decision_ts,
            "submit_ts_assumption": self.submit_ts_assumption,
            "side": self.side,
            "order_type": self.order_type,
            "reference_price": self.reference_price,
            "fill_reference_ts": self.fill_reference_ts,
            "fill_reference_price": self.fill_reference_price,
            "fill_reference_source": self.fill_reference_source,
            "signal_candle_start_ts": self.signal_candle_start_ts,
            "signal_candle_close_ts": self.signal_candle_close_ts,
            "signal_reference_price": self.signal_reference_price,
            "signal_reference_source": self.signal_reference_source,
            "quote_ts": self.quote_ts,
            "quote_age_ms": self.quote_age_ms,
            "quote_source": self.quote_source,
            "requested_qty": self.requested_qty,
            "filled_qty": self.filled_qty,
            "remaining_qty": self.remaining_qty,
            "avg_fill_price": self.avg_fill_price,
            "fee": self.fee,
            "slippage_bps": self.slippage_bps,
            "latency_ms": self.latency_ms,
            "fill_status": self.fill_status,
            "model_name": self.model_name,
            "model_version": self.model_version,
            "model_params_hash": self.model_params_hash,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "spread_bps": self.spread_bps,
            "orderbook_depth_ref": self.orderbook_depth_ref,
            "requested_notional": self.requested_notional,
            "filled_notional": self.filled_notional,
            "depth_snapshot_ts": self.depth_snapshot_ts,
            "depth_snapshot_age_ms": self.depth_snapshot_age_ms,
            "depth_levels_consumed": self.depth_levels_consumed,
            "depth_available": self.depth_available,
            "depth_sufficient": self.depth_sufficient,
            "queue_position_mode": self.queue_position_mode,
            "market_impact_mode": self.market_impact_mode,
            "execution_liquidity_evidence_type": self.execution_liquidity_evidence_type,
            "execution_realism_limitations": list(self.execution_realism_limitations),
            "execution_reality_level": self.execution_reality_level,
            "allow_same_candle_close_fill": self.allow_same_candle_close_fill,
            "quote_selection": self.quote_selection,
            "fill_reference_policy": self.fill_reference_policy,
            "top_of_book_source": self.top_of_book_source,
            "top_of_book_is_full_depth": self.top_of_book_is_full_depth,
            "execution_reference_failure_reason": self.execution_reference_failure_reason,
            "latency_applied_to_reference": self.latency_applied_to_reference,
            "latency_applied_to_submit_ts": self.latency_applied_to_submit_ts,
            "latency_applied_to_fill_reference": self.latency_applied_to_fill_reference,
            "latency_reference_policy_warning": self.latency_reference_policy_warning,
            "feature_snapshot": self.feature_snapshot,
            "regime_snapshot": self.regime_snapshot,
            "intra_candle_policy": self.intra_candle_policy,
            "base_seed": self.base_seed,
            "derived_seed_hash": self.derived_seed_hash,
            "seed_derivation_inputs": self.seed_derivation_inputs,
        }


class ExecutionModel(Protocol):
    name: str
    version: str

    def params_payload(self) -> dict[str, Any]:
        ...

    def simulate(self, request: ExecutionRequest) -> ExecutionFill:
        ...


def model_params_hash(payload: dict[str, Any]) -> str:
    return sha256_prefixed(payload)
