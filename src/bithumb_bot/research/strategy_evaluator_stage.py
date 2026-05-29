from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bithumb_bot.canonical_decision import canonical_payload_hash
from bithumb_bot.strategy_policy_contract import StrategyDecisionV2

from .backtest_stages import ReplayTick, StrategyEvaluationEnvelope


@dataclass(frozen=True)
class DefaultStrategyEvaluator:
    """Policy-evaluation authority boundary for the default research path."""

    def run(self, state: Any) -> Any:
        return state

    def evaluate(
        self,
        tick: ReplayTick,
        position_snapshot: Any,
        strategy_context: dict[str, object],
    ) -> StrategyEvaluationEnvelope:
        from .strategy_registry import resolve_research_strategy_plugin

        dataset = strategy_context["dataset"]
        strategy_name = str(strategy_context["strategy_name"])
        parameter_values = dict(strategy_context["parameter_values"])  # type: ignore[arg-type]
        fee_rate = float(strategy_context["fee_rate"])
        slippage_bps = float(strategy_context["slippage_bps"])
        active_exit_policy = dict(strategy_context["active_exit_policy"])  # type: ignore[arg-type]
        buy_fraction = float(strategy_context["buy_fraction"])
        run_context = strategy_context["run_context"]
        event = tick.event
        plugin = resolve_research_strategy_plugin(strategy_name)
        event_extra = event.extra_payload if isinstance(event.extra_payload, dict) else {}
        raw_signal = str(event.raw_signal or "HOLD").upper()
        raw_reason = str(event_extra.get("raw_reason") or event.reason)
        raw_filter_would_block = bool(event_extra.get("raw_filter_would_block", bool(event.blocked_filters)))
        entry_filter_blocked = bool(event_extra.get("entry_filter_blocked", False))
        entry_signal = str(event.entry_signal or raw_signal).upper()
        policy_materialization_mode = str(
            getattr(run_context, "policy_materialization_mode", "research_exploratory")
        )
        promotion_grade_policy_required = policy_materialization_mode != "research_exploratory"
        policy_builder_kwargs: dict[str, object] = {
            "event": event,
            "dataset": dataset,
            "candle_index": int(tick.candle_index),
            "position": position_snapshot,
            "parameter_values": parameter_values,
            "fee_rate": fee_rate,
            "slippage_bps": slippage_bps,
            "active_exit_policy": active_exit_policy,
            "buy_fraction": buy_fraction,
        }
        if plugin.policy_assembly_factory is not None:
            policy_builder_kwargs.update(
                {
                    "materialization_mode": policy_materialization_mode,
                    "candidate_regime_policy": (
                        dict(getattr(run_context, "candidate_regime_policy"))
                        if isinstance(getattr(run_context, "candidate_regime_policy", None), dict)
                        else None
                    ),
                    "candidate_regime_policy_enforced": bool(
                        getattr(run_context, "candidate_regime_policy_drives_research_execution", True)
                    ),
                }
            )
        builder = plugin.research_policy_decision_builder
        policy_decision = builder(**policy_builder_kwargs) if builder is not None else None
        evaluates_exit_policy = bool(
            isinstance(event.exit_intent, dict)
            and str(event.exit_intent.get("mode") or "") == "evaluate_exit_policy"
        )
        allows_legacy_event_first_exit_policy = "research_runtime_contract.v2" not in str(
            event.strategy_version or ""
        )
        unsupported_reason = ""
        if (
            builder is not None
            and policy_decision is None
            and not (evaluates_exit_policy and allows_legacy_event_first_exit_policy)
        ):
            unsupported_reason = "research_policy_decision_missing_not_comparable"
        if promotion_grade_policy_required and policy_decision is None:
            raise ValueError(unsupported_reason or "research_policy_decision_missing_not_comparable")
        if policy_decision is not None:
            if not isinstance(policy_decision, StrategyDecisionV2):
                if promotion_grade_policy_required:
                    raise ValueError("research_strategy_decision_not_typed:StrategyDecisionV2")
                unsupported_reason = "research_strategy_decision_not_typed_compatibility_fallback"
                policy_decision = None
        service_provenance: object | None = None
        if policy_decision is not None:
            trace = policy_decision.as_trace()
            replay_hash = str(trace.get("replay_fingerprint_hash") or "")
            service_provenance = trace.get("strategy_evaluation_provenance")
            missing = [
                name
                for name in (
                    "policy_hash",
                    "policy_contract_hash",
                    "policy_input_hash",
                    "policy_decision_hash",
                )
                if not str(getattr(policy_decision, name, "") or "").strip()
            ]
            if not replay_hash:
                missing.append("replay_fingerprint_hash")
            if not isinstance(service_provenance, dict):
                missing.append("strategy_evaluation_provenance")
            elif service_provenance.get("decision_boundary") != "StrategyDecisionService.evaluate":
                missing.append("strategy_evaluation_provenance.decision_boundary")
            if promotion_grade_policy_required and missing:
                raise ValueError("research_strategy_decision_promotion_fields_missing:" + ",".join(missing))
            entry_decision = policy_decision.entry_decision
            raw_signal = str(policy_decision.raw_signal or "HOLD").upper()
            raw_reason = str(policy_decision.raw_reason or raw_reason)
            raw_filter_would_block = bool(policy_decision.trace.get("raw_filter_would_block"))
            entry_filter_blocked = bool(policy_decision.trace.get("entry_blocked"))
            entry_signal = str(policy_decision.entry_signal or raw_signal).upper()
            exit_signal = str(policy_decision.exit_signal or raw_signal).upper()
            blocked_filters = tuple(policy_decision.blocked_filters)
            if not replay_hash:
                replay_hash = canonical_payload_hash(
                    {
                        "policy_input_hash": policy_decision.policy_input_hash,
                        "policy_decision_hash": policy_decision.policy_decision_hash,
                        "policy_contract_hash": policy_decision.policy_contract_hash,
                        "candle_ts": int(tick.candle_ts),
                    }
                )
        else:
            entry_decision = event_extra.get("entry_decision")
            exit_signal = str(event.exit_signal or event.raw_signal or "HOLD").upper()
            blocked_filters = tuple(event.blocked_filters)
            compatibility_fallback_reason_code = unsupported_reason or "legacy_research_event_decision_payload"
            replay_hash = canonical_payload_hash(
                {
                    "strategy": plugin.name,
                    "candle_ts": int(tick.candle_ts),
                    "decision_ts": int(tick.decision_ts),
                    "raw_signal": raw_signal,
                    "final_signal": str(event.final_signal or "HOLD").upper(),
                    "compatibility_fallback": True,
                    "compatibility_fallback_reason_code": compatibility_fallback_reason_code,
                }
            )
        if policy_decision is not None:
            compatibility_fallback_reason_code = ""
        provenance = {
            "stage_id": "strategy_evaluator",
            "strategy_name": plugin.name,
            "entry_decision": entry_decision,
            "raw_signal": raw_signal,
            "raw_reason": raw_reason,
            "raw_filter_would_block": raw_filter_would_block,
            "entry_filter_blocked": entry_filter_blocked,
            "entry_signal": entry_signal,
            "exit_signal": exit_signal,
            "blocked_filters": blocked_filters,
            "policy_materialization_mode": policy_materialization_mode,
            "promotion_grade_policy_required": promotion_grade_policy_required,
            "allows_legacy_event_first_exit_policy": allows_legacy_event_first_exit_policy,
            "evaluates_exit_policy": evaluates_exit_policy,
            "strategy_evaluation_provenance": (
                dict(service_provenance)
                if policy_decision is not None and isinstance(service_provenance, dict)
                else None
            ),
            "compatibility_fallback": policy_decision is None,
            "compatibility_fallback_reason_code": compatibility_fallback_reason_code,
            "compatibility_fallback_recommended_next_action": (
                "none"
                if policy_decision is not None
                else "regenerate_research_decisions_with_typed_strategy_decision"
            ),
            "allow_execution_compatibility_fallback": bool(
                policy_decision is None
                and not unsupported_reason
                and (builder is None or allows_legacy_event_first_exit_policy)
            ),
        }
        return StrategyEvaluationEnvelope(
            decision=policy_decision,
            provenance=provenance,
            replay_fingerprint_hash=replay_hash,
            unsupported_reason=unsupported_reason,
            compatibility_fallback=policy_decision is None,
            promotion_grade=bool(policy_decision is not None and not unsupported_reason),
            recommended_next_action=(
                "none"
                if policy_decision is not None
                else "regenerate_research_decisions_with_typed_strategy_decision"
            ),
        )


__all__ = ["DefaultStrategyEvaluator"]
