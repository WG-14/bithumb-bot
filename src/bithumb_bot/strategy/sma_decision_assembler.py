from __future__ import annotations

from dataclasses import asdict, replace

from bithumb_bot.core.sma_policy import (
    EntryExecutionIntent,
    ExecutionConstraintSnapshot,
    ExecutionIntentV1,
    ExitExecutionIntent,
    MarketWindow,
    PositionSnapshot,
    SmaPolicyConfig,
    StrategyDecisionV2,
    _stable_hash,
    _stable_position_terminal_state,
    evaluate_sma_policy,
)

from .exit_rules import ExitPolicyConfig, evaluate_sma_exit_policy


PROTECTIVE_EXIT_RULE_NAMES = frozenset({"stop_loss", "max_holding_time"})


def evaluate_sma_final_decision(
    *,
    market: MarketWindow,
    position: PositionSnapshot,
    config: SmaPolicyConfig,
    execution_context: ExecutionConstraintSnapshot,
    exit_policy_config: ExitPolicyConfig,
    signal_context_extra: dict[str, object] | None = None,
    rule_sources: dict[str, str] | None = None,
) -> StrategyDecisionV2:
    """Build the typed final SMA decision from immutable policy snapshots."""
    entry_decision = evaluate_sma_policy(
        market=market,
        position=position,
        config=config,
        execution_context=execution_context,
    )
    resolved_exit_signal = str(entry_decision.exit_signal or entry_decision.raw_signal).upper()
    resolved_exit_reason = str(entry_decision.exit_reason or entry_decision.raw_reason)
    final_signal = str(entry_decision.entry_signal or "HOLD").upper()
    final_reason = str(entry_decision.entry_reason or "")
    exit_rule: str | None = None
    exit_evaluations: tuple[dict[str, object], ...] = ()

    allow_harmless_dust_exit_evaluation = bool(
        position.dust_classification == "harmless_dust"
        and not position.effective_flat
        and position.in_position
    )
    if (
        (resolved_exit_signal == "SELL" or position.in_position)
        and not position.exit_allowed
        and not allow_harmless_dust_exit_evaluation
    ):
        final_signal = "HOLD"
        final_reason = str(position.exit_block_reason or "exit_blocked_by_position_state")
        exit_reason = final_reason
    elif position.in_position:
        exit_decision = evaluate_sma_exit_policy(
            position=position,
            market=market,
            raw_signal=entry_decision.raw_signal,
            raw_reason=entry_decision.raw_reason,
            entry_signal=entry_decision.entry_signal,
            exit_signal=resolved_exit_signal,
            config=exit_policy_config,
            signal_context_extra=signal_context_extra,
            rule_sources=rule_sources,
        )
        exit_evaluations = tuple(dict(item) for item in exit_decision.evaluations)
        exit_rule = exit_decision.rule
        exit_reason = exit_decision.reason
        if exit_decision.triggered:
            final_signal = "SELL"
            final_reason = exit_decision.reason
        else:
            final_signal = "HOLD"
            final_reason = "position held: no exit rule triggered"
    elif final_signal == "BUY" and not position.entry_allowed:
        final_signal = "HOLD"
        final_reason = str(position.entry_block_reason or "entry_blocked_by_position_state")
        exit_reason = resolved_exit_reason
    elif entry_decision.raw_signal == "SELL" and not position.in_position:
        final_signal = "HOLD"
        final_reason = str(position.exit_block_reason or "no_position")
        exit_reason = final_reason
    else:
        exit_reason = resolved_exit_reason

    raw_filter_would_block = bool(entry_decision.trace.get("raw_filter_would_block", False))
    entry_blocked = bool(entry_decision.raw_signal == "BUY" and final_signal == "HOLD")
    entry_block_reason = final_reason if entry_blocked else None
    protective_exit_overrode_entry = bool(
        entry_decision.raw_signal == "BUY"
        and final_signal == "SELL"
        and str(exit_rule or "").lower() in PROTECTIVE_EXIT_RULE_NAMES
    )
    exit_filter_suppression_prevented = bool(
        entry_decision.raw_signal == "SELL"
        and raw_filter_would_block
        and position.in_position
        and position.exit_allowed
        and resolved_exit_signal == "SELL"
        and bool(exit_evaluations)
    )
    typed_execution_intent = _execution_intent(
        final_signal=final_signal,
        market=market,
        config=config,
    )
    execution_intent = (
        typed_execution_intent.as_dict()
        if typed_execution_intent is not None
        else None
    )
    exit_policy_payload = exit_policy_config.policy_input_payload()
    policy_input_hash = _stable_hash(
        {
            "entry_policy_input_hash": entry_decision.policy_input_hash,
            "exit_policy": exit_policy_payload,
            "exit_policy_hash": _stable_hash(exit_policy_payload),
            "execution_sizing": (
                {
                    "buy_fraction": float(config.buy_fraction),
                    "max_order_krw": float(config.max_order_krw),
                }
                if typed_execution_intent is not None and typed_execution_intent.side == "BUY"
                else None
            ),
        }
    )
    trace = dict(entry_decision.trace)
    trace.update(
        {
            "schema_version": 2,
            "policy": "sma_with_filter_final_decision",
            "exit_signal": resolved_exit_signal,
            "exit_reason": exit_reason,
            "exit_policy": exit_policy_payload,
            "exit_policy_hash": _stable_hash(exit_policy_payload),
            "exit_rule": exit_rule,
            "exit_evaluations": [dict(item) for item in exit_evaluations],
            "final_signal": final_signal,
            "final_reason": final_reason,
            "entry_blocked": entry_blocked,
            "entry_block_reason": entry_block_reason,
            "protective_exit_overrode_entry": protective_exit_overrode_entry,
            "exit_filter_suppression_prevented": exit_filter_suppression_prevented,
            "execution_intent": execution_intent,
            "position": asdict(position),
        }
    )
    policy_hash = _stable_hash(trace)
    policy_decision_hash = _stable_hash(
        {
            "strategy_name": entry_decision.strategy_name,
            "raw_signal": entry_decision.raw_signal,
            "raw_reason": entry_decision.raw_reason,
            "entry_signal": entry_decision.entry_signal,
            "entry_reason": entry_decision.entry_reason,
            "exit_signal": resolved_exit_signal,
            "exit_reason": exit_reason,
            "final_signal": final_signal,
            "final_reason": final_reason,
            "blocked_filters": list(entry_decision.blocked_filters),
            "entry_blocked": entry_blocked,
            "entry_block_reason": entry_block_reason,
            "exit_rule": exit_rule,
            "exit_evaluations": [dict(item) for item in exit_evaluations],
            "protective_exit_overrode_entry": protective_exit_overrode_entry,
            "exit_filter_suppression_prevented": exit_filter_suppression_prevented,
            "position_terminal_state": _stable_position_terminal_state(position.terminal_state),
            "execution_intent": execution_intent,
        }
    )
    return replace(
        entry_decision,
        exit_signal=resolved_exit_signal,
        exit_reason=exit_reason,
        final_signal=final_signal,
        final_reason=final_reason,
        entry_blocked=entry_blocked,
        entry_block_reason=entry_block_reason,
        exit_rule=exit_rule,
        exit_evaluations=exit_evaluations,
        protective_exit_overrode_entry=protective_exit_overrode_entry,
        exit_filter_suppression_prevented=exit_filter_suppression_prevented,
        position_snapshot=position,
        execution_intent=typed_execution_intent,
        trace=trace,
        policy_hash=policy_hash,
        policy_input_hash=policy_input_hash,
        policy_decision_hash=policy_decision_hash,
    )


def _execution_intent(
    *,
    final_signal: str,
    market: MarketWindow,
    config: SmaPolicyConfig,
) -> ExecutionIntentV1 | None:
    if final_signal == "BUY":
        return EntryExecutionIntent(
            side="BUY",
            intent="enter_open_exposure",
            pair=market.pair,
            requires_execution_sizing=True,
            budget_fraction_of_cash=float(config.buy_fraction),
            max_budget_krw=float(config.max_order_krw),
        )
    if final_signal == "SELL":
        return ExitExecutionIntent(
            side="SELL",
            intent="exit_open_exposure",
            pair=market.pair,
            requires_execution_sizing=True,
        )
    return None
