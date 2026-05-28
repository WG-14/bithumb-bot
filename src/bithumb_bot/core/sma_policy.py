from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

from bithumb_bot.sma_decision import (
    SmaEntryDecision,
    evaluate_sma_entry_decision,
    evaluate_sma_entry_decision_from_features,
)
from bithumb_bot.strategy_policy_contract import (
    EntryExecutionIntent,
    ExecutionConstraintSnapshot,
    ExecutionIntentV1,
    ExitExecutionIntent,
    PositionSnapshot,
    Signal,
    StrategyDecisionV2,
)


def _stable_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _stable_execution_constraints_payload(payload: dict[str, object]) -> dict[str, object]:
    stable = dict(payload)
    fee_authority = stable.get("fee_authority")
    if isinstance(fee_authority, dict):
        stable["fee_authority"] = {
            key: value
            for key, value in fee_authority.items()
            if key not in {"retrieved_at_sec", "expires_at_sec"}
        }
    return stable


def _stable_position_terminal_state(value: object) -> object:
    state = str(value or "")
    if state == "research_simulated_flat":
        return "flat"
    if state == "research_simulated_open_exposure":
        return "open_exposure"
    return value


def _stable_position_policy_input(payload: dict[str, object]) -> dict[str, object]:
    keys = (
        "in_position",
        "entry_allowed",
        "exit_allowed",
        "entry_block_reason",
        "exit_block_reason",
        "terminal_state",
        "dust_classification",
        "dust_state",
        "effective_flat",
        "has_executable_exposure",
        "has_any_position_residue",
        "has_non_executable_residue",
        "has_dust_only_remainder",
    )
    stable = {key: payload.get(key) for key in keys}
    stable["terminal_state"] = _stable_position_terminal_state(stable.get("terminal_state"))
    return stable


def _stable_market_policy_input(payload: dict[str, object]) -> dict[str, object]:
    return {
        key: payload.get(key)
        for key in (
            "pair",
            "interval",
            "candle_ts",
            "through_ts_ms",
            "last_close",
            "prev_s",
            "prev_l",
            "curr_s",
            "curr_l",
            "previous_cross_state",
            "allow_initial_cross",
        )
    }


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

    def policy_input_payload(self) -> dict[str, object]:
        return {
            "pair": self.pair,
            "interval": self.interval,
            "candle_ts": int(self.candle_ts),
            "through_ts_ms": self.through_ts_ms,
            "last_close": float(self.closes[-1]) if self.closes else None,
            "prev_s": float(self.prev_s),
            "prev_l": float(self.prev_l),
            "curr_s": float(self.curr_s),
            "curr_l": float(self.curr_l),
            "previous_cross_state": self.previous_cross_state,
            "allow_initial_cross": bool(self.allow_initial_cross),
            "gap_ratio": self.gap_ratio,
            "volatility_ratio": self.volatility_ratio,
            "overextended_ratio": self.overextended_ratio,
            "market_regime_snapshot": dict(self.market_regime_snapshot or {}),
        }


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
    strategy_min_expected_edge_ratio: float = 0.0
    candidate_regime_policy: dict[str, object] | None = None
    require_candidate_regime_policy: bool = False
    candidate_regime_policy_enforced: bool | None = None
    candidate_regime_policy_status: dict[str, object] | None = None
    runtime_comparable: bool = True
    materialization_mode: str = "unspecified"

    def policy_input_payload(self) -> dict[str, object]:
        candidate_status = dict(self.candidate_regime_policy_status or {})
        if "candidate_regime_policy_hash" not in candidate_status:
            candidate_status["candidate_regime_policy_hash"] = (
                _stable_hash(dict(self.candidate_regime_policy))
                if isinstance(self.candidate_regime_policy, dict)
                else "sha256:missing"
            )
        candidate_status.setdefault(
            "candidate_regime_policy_required",
            bool(self.require_candidate_regime_policy),
        )
        candidate_status.setdefault(
            "candidate_regime_policy_loaded",
            isinstance(self.candidate_regime_policy, dict),
        )
        return {
            "strategy_name": self.strategy_name,
            "short_n": int(self.short_n),
            "long_n": int(self.long_n),
            "min_gap_ratio": float(self.min_gap_ratio),
            "volatility_window": int(self.volatility_window),
            "min_volatility_ratio": float(self.min_volatility_ratio),
            "overextended_lookback": int(self.overextended_lookback),
            "overextended_max_return_ratio": float(self.overextended_max_return_ratio),
            "slippage_bps": float(self.slippage_bps),
            "live_fee_rate_estimate": float(self.live_fee_rate_estimate),
            "entry_edge_buffer_ratio": float(self.entry_edge_buffer_ratio),
            "cost_edge_enabled": bool(self.cost_edge_enabled),
            "cost_edge_min_ratio": float(self.cost_edge_min_ratio),
            "strategy_min_expected_edge_ratio": float(self.strategy_min_expected_edge_ratio),
            "effective_cost_edge_min_ratio": max(
                float(self.cost_edge_min_ratio),
                float(self.strategy_min_expected_edge_ratio),
            ),
            "market_regime_enabled": bool(self.market_regime_enabled),
            "buy_fraction": float(self.buy_fraction),
            "max_order_krw": float(self.max_order_krw),
            "require_candidate_regime_policy": bool(self.require_candidate_regime_policy),
            "candidate_regime_policy_effective_required": (
                bool(self.require_candidate_regime_policy)
                if self.candidate_regime_policy_enforced is None
                else bool(self.candidate_regime_policy_enforced)
            ),
            "candidate_regime_policy_status": candidate_status,
            "runtime_comparable": bool(self.runtime_comparable),
            "materialization_equivalence_scope": (
                "runtime_comparable"
                if bool(self.runtime_comparable)
                else "research_exploratory_not_runtime_comparable"
            ),
        }


def evaluate_sma_policy(
    *,
    market: MarketWindow,
    position: PositionSnapshot,
    config: SmaPolicyConfig,
    execution_context: ExecutionConstraintSnapshot,
) -> StrategyDecisionV2:
    """Evaluate the entry SMA policy from immutable snapshots.

    This function is intentionally free of database, runtime config, clock, broker,
    notifier, and persistence dependencies. Runtime callers must perform state
    normalization before building ``PositionSnapshot``. This entry-policy result
    is not execution authority; execution planning must consume the final
    decision assembled by ``evaluate_sma_final_decision`` or an equivalent
    ``StrategyPolicy.decide_snapshot`` implementation.
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
            cost_edge_min_ratio=max(
                float(config.cost_edge_min_ratio),
                float(config.strategy_min_expected_edge_ratio),
            ),
            market_regime_enabled=bool(config.market_regime_enabled),
            candidate_regime_policy=(
                config.candidate_regime_policy
                if (
                    bool(config.require_candidate_regime_policy)
                    if config.candidate_regime_policy_enforced is None
                    else bool(config.candidate_regime_policy_enforced)
                )
                else None
            ),
            require_candidate_regime_policy=(
                bool(config.require_candidate_regime_policy)
                if config.candidate_regime_policy_enforced is None
                else bool(config.candidate_regime_policy_enforced)
            ),
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
            cost_edge_min_ratio=max(
                float(config.cost_edge_min_ratio),
                float(config.strategy_min_expected_edge_ratio),
            ),
            market_regime_enabled=bool(config.market_regime_enabled),
            candidate_regime_policy=(
                config.candidate_regime_policy
                if (
                    bool(config.require_candidate_regime_policy)
                    if config.candidate_regime_policy_enforced is None
                    else bool(config.candidate_regime_policy_enforced)
                )
                else None
            ),
            require_candidate_regime_policy=(
                bool(config.require_candidate_regime_policy)
                if config.candidate_regime_policy_enforced is None
                else bool(config.candidate_regime_policy_enforced)
            ),
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
        "config": config.policy_input_payload(),
        "execution_constraints": {
            "fee_rate_for_decision": float(execution_context.fee_rate_for_decision),
            "fee_authority_degraded_blocks_entry": bool(
                execution_context.fee_authority_degraded_blocks_entry
            ),
            "fee_authority": dict(execution_context.fee_authority),
            "order_rules": dict(execution_context.order_rules),
        },
    }
    config_payload = config.policy_input_payload()
    position_payload = position.policy_input_payload()
    position_payload["terminal_state"] = _stable_position_terminal_state(
        position_payload.get("terminal_state")
    )
    policy_input = {
        "market": _stable_market_policy_input(trace["market"]),  # type: ignore[arg-type]
        "position": position_payload,
        "execution_constraints": _stable_execution_constraints_payload(
            trace["execution_constraints"]  # type: ignore[arg-type]
        ),
        "config": config_payload,
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
            "position_terminal_state": _stable_position_terminal_state(position.terminal_state),
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
