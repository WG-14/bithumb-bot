from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from . import backtest_support as support


@dataclass(frozen=True)
class DecisionPayloadBuilder:
    """Builds non-authoritative research decision observability payloads."""

    def build(
        self,
        *,
        dataset: Any,
        dataset_content_hash: str,
        parameter_values: dict[str, Any],
        strategy_plugin: Any,
        strategy_spec: Any,
        exit_policy: dict[str, Any],
        exit_policy_hash: str,
        exit_policy_config_hash: str | None,
        fee_rate: float,
        slippage_bps: float,
        timing_policy: Any,
        portfolio_policy: Any,
        event: Any,
        decision_boundary_ts: int,
        strategy_envelope: Any,
        risk_decision: Any,
        policy_position: Any,
        policy_decision: Any | None,
        regime_snapshot: dict[str, object],
        qty: float,
        sellable_qty: float,
    ) -> dict[str, object]:
        action = risk_decision.final_signal
        raw_signal = str(strategy_envelope.provenance.get("raw_signal") or "HOLD").upper()
        raw_reason = str(strategy_envelope.provenance.get("raw_reason") or event.reason)
        raw_filter_would_block = bool(strategy_envelope.provenance.get("raw_filter_would_block"))
        entry_signal = str(strategy_envelope.provenance.get("entry_signal") or raw_signal).upper()
        exit_signal = str(strategy_envelope.provenance.get("exit_signal") or raw_signal).upper()
        blocked_filters = list(strategy_envelope.provenance.get("blocked_filters") or ())
        entry_decision = strategy_envelope.provenance.get("entry_decision")
        market_regime_decision = (
            dict(getattr(entry_decision, "candidate_regime_decision"))
            if entry_decision is not None
            and isinstance(getattr(entry_decision, "candidate_regime_decision", None), dict)
            else {"regime_decision": "not_configured"}
        )
        market_regime_blocked = bool(
            getattr(entry_decision, "market_regime_triggered", False) if entry_decision is not None else False
        )
        candidate_regime_blocked = bool(
            getattr(entry_decision, "candidate_regime_triggered", False) if entry_decision is not None else False
        )
        if policy_decision is not None:
            protective_exit_overrode_entry = bool(policy_decision.protective_exit_overrode_entry)
            entry_blocked = bool(policy_decision.entry_blocked)
            exit_filter_suppression_prevented = bool(policy_decision.exit_filter_suppression_prevented)
        elif strategy_envelope.unsupported_reason:
            protective_exit_overrode_entry = False
            entry_blocked = False
            exit_filter_suppression_prevented = False
        else:
            protective_exit_overrode_entry = bool(
                raw_signal == "BUY"
                and action == "SELL"
                and risk_decision.exit_rule in {"stop_loss", "max_holding_time"}
            )
            entry_blocked = bool(raw_signal == "BUY" and action == "HOLD" and raw_filter_would_block)
            exit_filter_suppression_prevented = bool(
                raw_signal == "SELL"
                and raw_filter_would_block
                and sellable_qty > 1e-12
                and bool(risk_decision.exit_evaluations)
            )
        payload = support.research_decision_payload(
            dataset=dataset,
            dataset_content_hash=dataset_content_hash,
            parameter_values=parameter_values,
            strategy_name=strategy_plugin.name,
            strategy_spec=strategy_spec.as_dict(),
            strategy_spec_hash=strategy_spec.spec_hash(),
            strategy_plugin_contract=strategy_plugin.contract_payload(),
            strategy_plugin_contract_hash=strategy_plugin.contract_hash(),
            exit_policy=exit_policy,
            exit_policy_hash=exit_policy_hash,
            exit_policy_config_hash=exit_policy_config_hash,
            fee_rate=fee_rate,
            slippage_bps=slippage_bps,
            timing_policy=timing_policy,
            portfolio_policy=portfolio_policy,
            candle_ts=event.candle_ts,
            decision_ts=decision_boundary_ts,
            raw_signal=raw_signal,
            entry_signal=entry_signal,
            exit_signal=exit_signal,
            final_signal=action,
            raw_reason=raw_reason,
            blocked=bool(risk_decision.block or (raw_signal in {"BUY", "SELL"} and action == "HOLD")),
            raw_filter_would_block=raw_filter_would_block,
            entry_blocked=entry_blocked,
            protective_exit_overrode_entry=protective_exit_overrode_entry,
            exit_filter_suppression_prevented=exit_filter_suppression_prevented,
            blocked_filters=blocked_filters,
            feature_snapshot=dict(event.feature_snapshot),
            regime_snapshot=regime_snapshot,
            entry_reason=risk_decision.reason_code,
            market_regime_decision=market_regime_decision,
            market_regime_blocked=market_regime_blocked,
            candidate_regime_blocked=candidate_regime_blocked,
            qty=qty,
            sellable_qty=sellable_qty,
            exit_rule=risk_decision.exit_rule,
            exit_reason=risk_decision.exit_reason,
            exit_evaluations=[dict(item) for item in risk_decision.exit_evaluations],
        )
        promotion_grade = bool(getattr(strategy_plugin, "is_promotion_grade", False))
        if strategy_plugin.decision_payload_adapter is not None:
            if promotion_grade:
                if policy_decision is not None:
                    payload = strategy_plugin.decision_payload_adapter(payload, policy_decision)
            else:
                payload = strategy_plugin.decision_payload_adapter(payload, event)
        promotion_missing_reason = (
            ""
            if promotion_grade
            else str(getattr(getattr(strategy_plugin, "runtime_capabilities", None), "fail_closed_reason", ""))
        )
        payload.update(
            {
                "decision_event_schema_version": 1,
                "strategy_decision_contract_version": strategy_plugin.decision_contract_version,
                "promotion_grade": promotion_grade,
                "promotion_extension_missing_reason": promotion_missing_reason,
                "recommended_next_action": "none" if promotion_grade else "promote_strategy_contract",
                "raw_reason": raw_reason,
                "feature_snapshot": dict(event.feature_snapshot),
                "strategy_diagnostics_namespace": strategy_plugin.diagnostics_namespace,
                "strategy_diagnostics": dict(event.strategy_diagnostics),
                "strategy_behavior_payload": {
                    "strategy_name": event.strategy_name,
                    "strategy_version": event.strategy_version,
                    "raw_signal": raw_signal,
                    "final_signal": action,
                    "reason": risk_decision.reason_code,
                    "feature_snapshot": dict(event.feature_snapshot),
                    "strategy_diagnostics": dict(event.strategy_diagnostics),
                },
                "execution_intent": action.lower() if action in {"BUY", "SELL"} else "none",
                "order_intent": dict(event.order_intent) if event.order_intent is not None else None,
                "exit_intent": dict(event.exit_intent) if event.exit_intent is not None else None,
                "research_policy_position_terminal_state": policy_position.terminal_state,
                "research_policy_recomputed_with_simulated_position": policy_decision is not None,
                "research_policy_unsupported": bool(strategy_envelope.unsupported_reason),
                "research_policy_unsupported_reason": strategy_envelope.unsupported_reason,
                "research_policy_comparable": not bool(strategy_envelope.unsupported_reason),
                "runtime_comparable": bool(strategy_envelope.provenance.get("runtime_comparable")),
            }
        )
        risk_payload = risk_decision.payload if isinstance(risk_decision.payload, dict) else {}
        for key in (
            "risk_input_hash",
            "risk_policy_hash",
            "risk_evidence_hash",
            "risk_decision_hash",
            "risk_reason_code",
            "risk_status",
            "risk_evaluation_point",
            "risk_state_source",
            "effective_risk_limits",
        ):
            if key in risk_payload:
                payload[key] = risk_payload[key]
        if "risk_decision" in risk_payload:
            payload["risk_decision"] = risk_payload["risk_decision"]
        if strategy_plugin.name == "sma_with_filter" and "strategy_diagnostic_counts" not in payload:
            from bithumb_bot.research.sma_with_filter_plugin import (
                _diagnostic_count_defaults,
                _diagnostic_counts,
            )

            payload["strategy_diagnostic_count_defaults"] = _diagnostic_count_defaults()
            payload["strategy_diagnostic_counts"] = _diagnostic_counts(payload)
            payload["strategy_diagnostic_counts_authority"] = "diagnostic_non_authoritative"
        if policy_decision is not None:
            payload["pure_policy_hash"] = policy_decision.policy_hash
            payload["policy_contract_hash"] = policy_decision.policy_contract_hash
            payload["policy_input_hash"] = policy_decision.policy_input_hash
            payload["policy_decision_hash"] = policy_decision.policy_decision_hash
            payload["pure_policy_trace"] = policy_decision.as_trace()
            trace = policy_decision.as_trace()
            for key in (
                "decision_input_bundle_hash",
                "decision_input_contract_hash",
                "decision_input_bundle_payload_hash",
                "snapshot_projector_version",
                "snapshot_projector_hash",
                "materialized_parameters_hash",
                "market_snapshot_hash",
                "market_feature_hash",
                "canonical_feature_projection_hash",
                "final_exit_decision_input_hash",
                "position_snapshot_hash",
                "execution_constraints_hash",
                "policy_config_hash",
                "replay_fingerprint_hash",
            ):
                if str(trace.get(key) or "").strip():
                    payload[key] = trace[key]
            service_provenance = trace.get("strategy_evaluation_provenance")
            if isinstance(service_provenance, dict):
                payload["strategy_evaluation_provenance"] = dict(service_provenance)
            payload["execution_intent_v2"] = (
                policy_decision.execution_intent.as_dict()
                if policy_decision.execution_intent is not None
                else None
            )
            diagnostics = (
                dict(payload["strategy_diagnostics"])
                if isinstance(payload.get("strategy_diagnostics"), dict)
                else {}
            )
            diagnostics.update(
                {
                    "pure_policy_hash": policy_decision.policy_hash,
                    "policy_contract_hash": policy_decision.policy_contract_hash,
                    "policy_input_hash": policy_decision.policy_input_hash,
                    "policy_decision_hash": policy_decision.policy_decision_hash,
                    "pure_policy_trace": policy_decision.as_trace(),
                    "policy_position_terminal_state": policy_position.terminal_state,
                    "policy_recomputed_with_simulated_position": True,
                }
            )
            payload["strategy_diagnostics"] = diagnostics
        if str(exit_policy_config_hash or "").strip() and not str(
            payload.get("exit_policy_config_hash") or ""
        ).strip():
            payload["exit_policy_config_hash"] = str(exit_policy_config_hash)
        return payload


__all__ = ["DecisionPayloadBuilder"]
