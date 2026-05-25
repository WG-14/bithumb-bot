from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any

from bithumb_bot.sma_decision import (
    SmaEntryDecision,
    evaluate_sma_entry_decision,
    evaluate_sma_entry_decision_from_features,
)


Signal = str


def _stable_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


SMA_POLICY_CONTRACT_HASH = _stable_hash(
    {
        "contract": "sma_with_filter_final_decision",
        "version": 1,
        "authority": "typed_final_strategy_decision",
    }
)


@dataclass(frozen=True)
class MarketWindow:
    pair: str
    interval: str
    candle_ts: int
    closes: tuple[float, ...]
    prev_s: float
    prev_l: float
    curr_s: float
    curr_l: float
    through_ts_ms: int | None = None
    gap_ratio: float | None = None
    volatility_ratio: float | None = None
    overextended_ratio: float | None = None
    market_regime_snapshot: dict[str, object] | None = None
    entry_decision: SmaEntryDecision | None = None
    previous_cross_state: str | None = None
    allow_initial_cross: bool = True


@dataclass(frozen=True)
class SmaPolicyConfig:
    strategy_name: str
    short_n: int
    long_n: int
    min_gap_ratio: float
    volatility_window: int
    min_volatility_ratio: float
    overextended_lookback: int
    overextended_max_return_ratio: float
    slippage_bps: float
    live_fee_rate_estimate: float
    entry_edge_buffer_ratio: float
    cost_edge_enabled: bool
    cost_edge_min_ratio: float
    market_regime_enabled: bool
    buy_fraction: float
    max_order_krw: float
    candidate_regime_policy: dict[str, object] | None = None
    require_candidate_regime_policy: bool = False


@dataclass(frozen=True)
class PositionSnapshot:
    in_position: bool
    entry_allowed: bool
    exit_allowed: bool
    entry_block_reason: str = ""
    exit_block_reason: str = ""
    terminal_state: str = "flat"
    entry_ts: int | None = None
    entry_price: float | None = None
    qty_open: float = 0.0
    holding_time_sec: float = 0.0
    unrealized_pnl: float = 0.0
    unrealized_pnl_ratio: float = 0.0
    raw_qty_open: float = 0.0
    raw_total_asset_qty: float = 0.0
    open_lot_count: int = 0
    dust_tracking_lot_count: int = 0
    reserved_exit_lot_count: int = 0
    sellable_executable_lot_count: int = 0
    dust_classification: str = ""
    dust_state: str = ""
    effective_flat: bool = True
    has_executable_exposure: bool = False
    has_any_position_residue: bool = False
    has_non_executable_residue: bool = False
    has_dust_only_remainder: bool = False


@dataclass(frozen=True)
class ExecutionConstraintSnapshot:
    fee_rate_for_decision: float
    fee_authority_degraded_blocks_entry: bool = False
    fee_authority: dict[str, object] = field(default_factory=dict)
    order_rules: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyDecisionV2:
    strategy_name: str
    raw_signal: Signal
    raw_reason: str
    entry_signal: Signal
    entry_reason: str
    exit_signal: Signal
    exit_reason: str
    final_signal: Signal
    final_reason: str
    blocked_filters: tuple[str, ...]
    entry_blocked: bool
    entry_block_reason: str | None
    exit_rule: str | None
    exit_evaluations: tuple[dict[str, object], ...]
    protective_exit_overrode_entry: bool
    exit_filter_suppression_prevented: bool
    position_snapshot: PositionSnapshot
    execution_intent: dict[str, object] | None
    entry_decision: SmaEntryDecision
    trace: dict[str, object]
    policy_hash: str
    policy_contract_hash: str
    policy_input_hash: str
    policy_decision_hash: str

    def as_trace(self) -> dict[str, object]:
        payload = dict(self.trace)
        payload["policy_hash"] = self.policy_hash
        payload["policy_contract_hash"] = self.policy_contract_hash
        payload["policy_input_hash"] = self.policy_input_hash
        payload["policy_decision_hash"] = self.policy_decision_hash
        return payload


def evaluate_sma_policy(
    *,
    market: MarketWindow,
    position: PositionSnapshot,
    config: SmaPolicyConfig,
    execution_context: ExecutionConstraintSnapshot,
) -> StrategyDecisionV2:
    """Evaluate the pure SMA policy from immutable snapshots.

    This function is intentionally free of database, runtime config, clock, broker,
    notifier, and persistence dependencies. Runtime callers must perform state
    normalization before building ``PositionSnapshot``.
    """
    if market.entry_decision is not None:
        entry_decision = market.entry_decision
    elif (
        market.gap_ratio is not None
        and market.volatility_ratio is not None
        and market.overextended_ratio is not None
        and market.market_regime_snapshot is not None
    ):
        entry_decision = evaluate_sma_entry_decision_from_features(
            prev_s=float(market.prev_s),
            prev_l=float(market.prev_l),
            curr_s=float(market.curr_s),
            curr_l=float(market.curr_l),
            gap_ratio=float(market.gap_ratio),
            volatility_ratio=float(market.volatility_ratio),
            overextended_ratio=float(market.overextended_ratio),
            market_regime_snapshot=dict(market.market_regime_snapshot),
            min_gap_ratio=float(config.min_gap_ratio),
            min_volatility_ratio=float(config.min_volatility_ratio),
            overextended_max_return_ratio=float(config.overextended_max_return_ratio),
            slippage_bps=float(config.slippage_bps),
            live_fee_rate_estimate=float(execution_context.fee_rate_for_decision),
            entry_edge_buffer_ratio=float(config.entry_edge_buffer_ratio),
            cost_edge_enabled=bool(config.cost_edge_enabled),
            cost_edge_min_ratio=float(config.cost_edge_min_ratio),
            market_regime_enabled=bool(config.market_regime_enabled),
            candidate_regime_policy=config.candidate_regime_policy,
            require_candidate_regime_policy=bool(config.require_candidate_regime_policy),
            fee_authority_degraded_blocks_entry=bool(
                execution_context.fee_authority_degraded_blocks_entry
            ),
        )
    else:
        entry_decision = evaluate_sma_entry_decision(
            closes=market.closes,
            prev_s=float(market.prev_s),
            prev_l=float(market.prev_l),
            curr_s=float(market.curr_s),
            curr_l=float(market.curr_l),
            min_gap_ratio=float(config.min_gap_ratio),
            volatility_window=int(config.volatility_window),
            min_volatility_ratio=float(config.min_volatility_ratio),
            overextended_lookback=int(config.overextended_lookback),
            overextended_max_return_ratio=float(config.overextended_max_return_ratio),
            slippage_bps=float(config.slippage_bps),
            live_fee_rate_estimate=float(execution_context.fee_rate_for_decision),
            entry_edge_buffer_ratio=float(config.entry_edge_buffer_ratio),
            cost_edge_enabled=bool(config.cost_edge_enabled),
            cost_edge_min_ratio=float(config.cost_edge_min_ratio),
            market_regime_enabled=bool(config.market_regime_enabled),
            candidate_regime_policy=config.candidate_regime_policy,
            require_candidate_regime_policy=bool(config.require_candidate_regime_policy),
            fee_authority_degraded_blocks_entry=bool(
                execution_context.fee_authority_degraded_blocks_entry
            ),
        )

    raw_signal = entry_decision.base_signal
    raw_reason = entry_decision.base_reason
    entry_signal = entry_decision.entry_signal
    entry_reason = entry_decision.entry_reason
    cross_state = str(market.previous_cross_state or "").strip().lower()
    cross_state_overrode_entry = False
    if cross_state in {"above", "below", "unknown"}:
        current_above = bool(float(market.curr_s) > float(market.curr_l))
        if cross_state == "unknown" and bool(market.allow_initial_cross):
            resolved_raw_signal = raw_signal
            resolved_raw_reason = raw_reason
        elif cross_state == "unknown" and not bool(market.allow_initial_cross):
            resolved_raw_signal = "HOLD"
            resolved_raw_reason = "initial cross suppressed until previous cross state is known"
        elif cross_state == "below" and current_above:
            resolved_raw_signal = "BUY"
            resolved_raw_reason = "sma golden cross"
        elif cross_state == "above" and not current_above:
            resolved_raw_signal = "SELL"
            resolved_raw_reason = "sma dead cross"
        else:
            resolved_raw_signal = "HOLD"
            resolved_raw_reason = "sma no crossover"
        if resolved_raw_signal != raw_signal:
            cross_state_overrode_entry = True
        raw_signal = resolved_raw_signal
        raw_reason = resolved_raw_reason
        if raw_signal == "HOLD":
            entry_signal = "HOLD"
            entry_reason = raw_reason
        elif raw_signal == "SELL":
            entry_signal = "SELL"
            entry_reason = raw_reason
    exit_signal = raw_signal
    exit_reason = raw_reason
    final_signal = entry_signal
    final_reason = entry_reason

    if position.in_position:
        final_signal = "HOLD"
        final_reason = "position held: exit policy evaluation required"
    elif entry_signal == "BUY" and not position.entry_allowed:
        final_signal = "HOLD"
        final_reason = position.entry_block_reason or "entry_blocked_by_position_state"

    resolved_blocked_filters = (
        ()
        if cross_state_overrode_entry and raw_signal == "HOLD"
        else entry_decision.blocked_filters
    )
    resolved_raw_filter_would_block = bool(
        raw_signal in {"BUY", "SELL"}
        and (
            resolved_blocked_filters
            or entry_decision.market_regime_triggered
            or entry_decision.candidate_regime_triggered
        )
    )
    resolved_entry_blocked = bool(raw_signal == "BUY" and resolved_raw_filter_would_block)

    trace: dict[str, object] = {
        "schema_version": 1,
        "policy": "sma_with_filter_pure_policy",
        "strategy_name": config.strategy_name,
        "market": {
            "pair": market.pair,
            "interval": market.interval,
            "candle_ts": int(market.candle_ts),
            "through_ts_ms": market.through_ts_ms,
            "last_close": float(market.closes[-1]) if market.closes else None,
            "prev_s": float(market.prev_s),
            "prev_l": float(market.prev_l),
            "curr_s": float(market.curr_s),
            "curr_l": float(market.curr_l),
            "previous_cross_state": market.previous_cross_state,
            "allow_initial_cross": bool(market.allow_initial_cross),
        },
        "raw_signal": raw_signal,
        "raw_reason": raw_reason,
        "entry_signal": entry_signal,
        "entry_reason": entry_reason,
        "exit_signal": exit_signal,
        "exit_reason": exit_reason,
        "final_signal": final_signal,
        "final_reason": final_reason,
        "blocked_filters": list(resolved_blocked_filters),
        "entry_blocked": bool(resolved_entry_blocked),
        "raw_filter_would_block": bool(resolved_raw_filter_would_block),
        "market_regime_blocked": bool(entry_decision.market_regime_triggered),
        "candidate_regime_blocked": bool(entry_decision.candidate_regime_triggered),
        "position": asdict(position),
        "execution_constraints": {
            "fee_rate_for_decision": float(execution_context.fee_rate_for_decision),
            "fee_authority_degraded_blocks_entry": bool(
                execution_context.fee_authority_degraded_blocks_entry
            ),
            "fee_authority": dict(execution_context.fee_authority),
            "order_rules": dict(execution_context.order_rules),
        },
    }
    policy_input = {
        "market": trace["market"],
        "position": trace["position"],
        "execution_constraints": trace["execution_constraints"],
        "config": {
            "strategy_name": config.strategy_name,
            "short_n": int(config.short_n),
            "long_n": int(config.long_n),
            "min_gap_ratio": float(config.min_gap_ratio),
            "volatility_window": int(config.volatility_window),
            "min_volatility_ratio": float(config.min_volatility_ratio),
            "overextended_lookback": int(config.overextended_lookback),
            "overextended_max_return_ratio": float(config.overextended_max_return_ratio),
            "slippage_bps": float(config.slippage_bps),
            "live_fee_rate_estimate": float(config.live_fee_rate_estimate),
            "entry_edge_buffer_ratio": float(config.entry_edge_buffer_ratio),
            "cost_edge_enabled": bool(config.cost_edge_enabled),
            "cost_edge_min_ratio": float(config.cost_edge_min_ratio),
            "market_regime_enabled": bool(config.market_regime_enabled),
            "buy_fraction": float(config.buy_fraction),
            "max_order_krw": float(config.max_order_krw),
            "candidate_regime_policy": config.candidate_regime_policy,
            "require_candidate_regime_policy": bool(config.require_candidate_regime_policy),
        },
    }
    policy_hash = _stable_hash(trace)
    policy_input_hash = _stable_hash(policy_input)
    policy_decision_hash = _stable_hash(
        {
            "strategy_name": config.strategy_name,
            "raw_signal": raw_signal,
            "raw_reason": raw_reason,
            "entry_signal": entry_signal,
            "entry_reason": entry_reason,
            "exit_signal": exit_signal,
            "exit_reason": exit_reason,
            "final_signal": final_signal,
            "final_reason": final_reason,
            "blocked_filters": list(resolved_blocked_filters),
            "entry_blocked": bool(resolved_entry_blocked),
            "entry_block_reason": (
                position.entry_block_reason
                if bool(resolved_entry_blocked) and position.entry_block_reason
                else None
            ),
            "position_terminal_state": position.terminal_state,
        }
    )
    return StrategyDecisionV2(
        strategy_name=config.strategy_name,
        raw_signal=raw_signal,
        raw_reason=raw_reason,
        entry_signal=entry_signal,
        entry_reason=entry_reason,
        exit_signal=exit_signal,
        exit_reason=exit_reason,
        final_signal=final_signal,
        final_reason=final_reason,
        blocked_filters=tuple(resolved_blocked_filters),
        entry_blocked=bool(resolved_entry_blocked),
        entry_block_reason=(
            position.entry_block_reason
            if bool(resolved_entry_blocked) and position.entry_block_reason
            else None
        ),
        exit_rule=None,
        exit_evaluations=(),
        protective_exit_overrode_entry=False,
        exit_filter_suppression_prevented=False,
        position_snapshot=position,
        execution_intent=None,
        entry_decision=entry_decision,
        trace=trace,
        policy_hash=policy_hash,
        policy_contract_hash=SMA_POLICY_CONTRACT_HASH,
        policy_input_hash=policy_input_hash,
        policy_decision_hash=policy_decision_hash,
    )
