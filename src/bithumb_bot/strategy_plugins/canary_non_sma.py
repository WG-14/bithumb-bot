from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bithumb_bot.canonical_decision import canonical_payload_hash
from bithumb_bot.decision_equivalence import sha256_prefixed
from bithumb_bot.research.strategy_registry import RuntimeParameterAdapter
from bithumb_bot.research.strategy_spec import (
    StrategyParameterSchema,
    StrategySpec,
    materialize_strategy_parameters,
)
from bithumb_bot.runtime_data_provider import (
    RuntimeDataRequirementResolver,
    RuntimeFeatureSnapshot,
    SQLiteRuntimeDataProvider,
)
from bithumb_bot.runtime_decision_contract import RuntimeStrategyPolicyHashes
from bithumb_bot.strategy_authoring import (
    PromotionGradeStrategyExtension,
    build_live_eligible_strategy_plugin,
    research_plugin_from_event_builder,
)
from bithumb_bot.strategy_decision_service import StrategyDecisionService, StrategyEvaluationRequest
from bithumb_bot.strategy_evidence import StrategyDecisionEvidenceBuilder
from bithumb_bot.strategy_policy_contract import (
    EntryExecutionIntent,
    ExecutionConstraintSnapshot,
    PositionSnapshot,
    StrategyDecisionV2,
)


CANARY_NON_SMA_STRATEGY_NAME = "canary_non_sma"
CANARY_NON_SMA_POLICY_CONTRACT_VERSION = "canary_non_sma.order_intent_policy.v1"
CANARY_DEFAULT_REASON = "canary_non_sma_order_contract"


CANARY_NON_SMA_SPEC = StrategySpec(
    strategy_name=CANARY_NON_SMA_STRATEGY_NAME,
    strategy_version="canary_non_sma.promotion_contract.v2",
    accepted_parameter_names=(
        "CANARY_ORDER_START_INDEX",
        "CANARY_ORDER_SIDE",
        "CANARY_ORDER_REASON",
        "CANARY_DECISION_START_INDEX",
        "CANARY_REASON",
    ),
    required_parameter_names=(),
    behavior_affecting_parameter_names=(
        "CANARY_ORDER_START_INDEX",
        "CANARY_ORDER_SIDE",
        "CANARY_ORDER_REASON",
    ),
    metadata_only_parameter_names=(),
    research_only_parameter_names=("CANARY_DECISION_START_INDEX", "CANARY_REASON"),
    default_parameters={
        "CANARY_ORDER_START_INDEX": 0,
        "CANARY_ORDER_SIDE": "BUY",
        "CANARY_ORDER_REASON": CANARY_DEFAULT_REASON,
    },
    decision_contract_version="research_canary_non_sma_decision_contract.v2",
    required_data=("candles",),
    optional_data=(),
    exit_policy_schema={
        "schema_version": 1,
        "rules": (),
        "order_intent_capability_contract": {
            "can_emit_order_intent": True,
            "live_real_order_allowed": False,
            "reason": "architecture canary proves promotion-grade replay without live real-order authority",
        },
        "research_replay_sequence_policy": {
            "schema_version": 1,
            "pre_start_research_hold_events": "omitted",
            "runtime_replay_pre_start_decision": "HOLD",
            "equivalence_interpretation": "pre_start_hold_omission_is_deterministic_and_not_replay_mismatch",
        },
    },
    parameter_schema=(
        StrategyParameterSchema(
            name="CANARY_ORDER_START_INDEX",
            value_type="int",
            min_value=0,
            unit="candle_index",
            required=True,
            runtime_bound=True,
            behavior_affecting=True,
        ),
        StrategyParameterSchema(
            name="CANARY_ORDER_SIDE",
            value_type="str",
            enum=("BUY", "SELL", "HOLD"),
            unit="signal",
            required=True,
            runtime_bound=True,
            behavior_affecting=True,
        ),
        StrategyParameterSchema(
            name="CANARY_ORDER_REASON",
            value_type="str",
            unit="reason",
            required=True,
            runtime_bound=True,
            behavior_affecting=True,
        ),
        StrategyParameterSchema(
            name="CANARY_DECISION_START_INDEX",
            value_type="int",
            min_value=0,
            unit="candle_index",
            runtime_bound=False,
            behavior_affecting=False,
            deprecated_keys=("CANARY_ORDER_START_INDEX",),
            migration_rule="use CANARY_ORDER_START_INDEX",
        ),
        StrategyParameterSchema(
            name="CANARY_REASON",
            value_type="str",
            unit="reason",
            runtime_bound=False,
            behavior_affecting=False,
            deprecated_keys=("CANARY_ORDER_REASON",),
            migration_rule="use CANARY_ORDER_REASON",
        ),
    ),
)


@dataclass(frozen=True)
class CanaryLegacyDecision:
    signal: str
    reason: str
    context: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        payload = dict(self.context)
        payload["signal"] = self.signal
        payload["reason"] = self.reason
        return payload


@dataclass(frozen=True)
class CanaryNonSmaRuntimeDecisionResult:
    decision: StrategyDecisionV2
    base_context: dict[str, object]
    candle_ts: int
    market_price: float
    replay_fingerprint: dict[str, object]
    boundary: dict[str, object]
    policy_hashes: RuntimeStrategyPolicyHashes

    def legacy_strategy_decision(self) -> CanaryLegacyDecision:
        return CanaryLegacyDecision(
            signal=str(self.decision.final_signal),
            reason=str(self.decision.final_reason),
            context=dict(self.as_legacy_dict()),
        )

    def as_legacy_dict(self) -> dict[str, object]:
        payload = dict(self.base_context)
        payload.setdefault("strategy", self.decision.strategy_name)
        payload.setdefault("signal", self.decision.final_signal)
        payload.setdefault("reason", self.decision.final_reason)
        payload.setdefault("raw_signal", self.decision.raw_signal)
        payload.setdefault("raw_reason", self.decision.raw_reason)
        payload.setdefault("final_signal", self.decision.final_signal)
        payload.setdefault("final_reason", self.decision.final_reason)
        payload.setdefault("ts", int(self.candle_ts))
        payload.setdefault("last_close", float(self.market_price))
        payload.update(self.policy_hashes.as_dict())
        payload.setdefault("replay_fingerprint", dict(self.replay_fingerprint))
        payload.setdefault("boundary", dict(self.boundary))
        return payload


def _normalize_canary_parameters(parameters: dict[str, Any]) -> dict[str, Any]:
    start_index = parameters.get(
        "CANARY_ORDER_START_INDEX",
        parameters.get("CANARY_DECISION_START_INDEX", 0),
    )
    reason = parameters.get(
        "CANARY_ORDER_REASON",
        parameters.get("CANARY_REASON", CANARY_DEFAULT_REASON),
    )
    side = str(parameters.get("CANARY_ORDER_SIDE") or "BUY").strip().upper()
    if side not in {"BUY", "SELL", "HOLD"}:
        raise ValueError(f"canary_order_side_unsupported:{side or 'missing'}")
    return {
        "CANARY_ORDER_START_INDEX": max(0, int(start_index or 0)),
        "CANARY_ORDER_SIDE": side,
        "CANARY_ORDER_REASON": str(reason or CANARY_DEFAULT_REASON),
    }


def _canary_result(
    *,
    pair: str,
    interval: str,
    candle_ts: int,
    market_price: float,
    candle_index: int,
    parameters: dict[str, Any],
    request: Any | None = None,
    evaluation_mode: str = "runtime_replay",
    feature_snapshot: RuntimeFeatureSnapshot | None = None,
) -> CanaryNonSmaRuntimeDecisionResult:
    return _evaluate_canary_result(
        pair=pair,
        interval=interval,
        candle_ts=candle_ts,
        market_price=market_price,
        candle_index=candle_index,
        parameters=parameters,
        request=request,
        evaluation_mode=evaluation_mode,
        feature_snapshot=feature_snapshot,
    )


def _canary_policy_material(
    *,
    pair: str,
    interval: str,
    candle_ts: int,
    market_price: float,
    candle_index: int,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    resolved = _normalize_canary_parameters(parameters)
    side = str(resolved["CANARY_ORDER_SIDE"])
    reason = str(resolved["CANARY_ORDER_REASON"])
    start_index = int(resolved["CANARY_ORDER_START_INDEX"])
    final_signal = side if candle_index >= start_index else "HOLD"
    final_reason = reason if final_signal in {"BUY", "SELL"} else "canary_before_order_start_index"
    execution_intent = (
        EntryExecutionIntent(
            side="BUY",
            intent="enter_strategy_position",
            pair=pair,
            requires_execution_sizing=True,
            budget_fraction_of_cash=0.01,
            max_budget_krw=10_000.0,
        )
        if final_signal == "BUY"
        else None
    )
    policy_contract = {
        "schema_version": 1,
        "strategy_name": CANARY_NON_SMA_STRATEGY_NAME,
        "policy_contract_version": CANARY_NON_SMA_POLICY_CONTRACT_VERSION,
        "can_emit_order_intent": True,
        "live_real_order_allowed": False,
    }
    policy_input = {
        "schema_version": 1,
        "strategy_name": CANARY_NON_SMA_STRATEGY_NAME,
        "pair": pair,
        "interval": interval,
        "candle_ts": int(candle_ts),
        "candle_index": int(candle_index),
        "market_price": float(market_price),
        "parameters": dict(resolved),
    }
    policy_decision = {
        "schema_version": 1,
        "raw_signal": final_signal,
        "final_signal": final_signal,
        "final_reason": final_reason,
        "execution_intent": execution_intent.as_dict() if execution_intent is not None else None,
    }
    evidence = StrategyDecisionEvidenceBuilder().build(
        strategy_name=CANARY_NON_SMA_STRATEGY_NAME,
        policy_contract_material=policy_contract,
        policy_input_material=policy_input,
        policy_decision_material=policy_decision,
        replay_fingerprint_material={
            "policy_contract_version": CANARY_NON_SMA_POLICY_CONTRACT_VERSION,
            "candle_ts": int(candle_ts),
            "candle_index": int(candle_index),
            "market_price": float(market_price),
            "parameters": dict(resolved),
            "decision_input_bundle_hash": sha256_prefixed(policy_input),
            "decision_input_contract_hash": sha256_prefixed(policy_contract),
            "decision_input_bundle_payload_hash": sha256_prefixed(
                {
                    "policy_input": policy_input,
                    "policy_decision": policy_decision,
                }
            ),
            "market_snapshot_hash": sha256_prefixed(
                {
                    "pair": pair,
                    "interval": interval,
                    "candle_ts": int(candle_ts),
                    "market_price": float(market_price),
                }
            ),
            "market_feature_hash": sha256_prefixed(
                {
                    "candle_index": int(candle_index),
                    "market_price": float(market_price),
                    "feature_family": "canary_close_only",
                }
            ),
            "canonical_feature_projection_hash": sha256_prefixed(
                {
                    "candle_index": int(candle_index),
                    "market_price": float(market_price),
                    "feature_family": "canary_close_only",
                }
            ),
            "position_snapshot_hash": sha256_prefixed(
                {"in_position": False, "entry_allowed": True, "exit_allowed": False}
            ),
            "execution_constraints_hash": sha256_prefixed({"fee_rate_for_decision": 0.0}),
            "policy_config_hash": sha256_prefixed({"parameters": dict(resolved)}),
            "exit_policy_config_hash": sha256_prefixed({"schema_version": 1, "rules": ()}),
            "final_exit_decision_input_hash": sha256_prefixed(policy_decision),
            "snapshot_projector_version": "canary_non_sma_snapshot_v1",
            "snapshot_projector_hash": sha256_prefixed(
                {
                    "strategy_name": CANARY_NON_SMA_STRATEGY_NAME,
                    "projector": "canary_non_sma_snapshot_v1",
                }
            ),
        },
    )
    return {
        "resolved": resolved,
        "final_signal": final_signal,
        "final_reason": final_reason,
        "execution_intent": execution_intent,
        "policy_contract": policy_contract,
        "policy_input": policy_input,
        "policy_decision": policy_decision,
        "policy_hash": evidence.policy_hash,
        "policy_contract_hash": evidence.policy_contract_hash,
        "policy_input_hash": evidence.policy_input_hash,
        "policy_decision_hash": evidence.policy_decision_hash,
        "replay_fingerprint": dict(evidence.replay_fingerprint),
        "replay_fingerprint_hash": evidence.replay_fingerprint_hash,
    }


@dataclass(frozen=True)
class CanaryNonSmaPolicy:
    name: str = CANARY_NON_SMA_STRATEGY_NAME

    def decide_snapshot(
        self,
        *,
        market: object,
        position: PositionSnapshot,
        config: object,
        execution_context: ExecutionConstraintSnapshot,
        exit_policy_config: object | None = None,
        rule_sources: dict[str, str] | None = None,
    ) -> StrategyDecisionV2:
        del position, execution_context, exit_policy_config, rule_sources
        market_payload = dict(market) if isinstance(market, dict) else {}
        config_payload = dict(config) if isinstance(config, dict) else {}
        material = _canary_policy_material(
            pair=str(market_payload.get("pair") or ""),
            interval=str(market_payload.get("interval") or ""),
            candle_ts=int(market_payload.get("candle_ts") or 0),
            market_price=float(market_payload.get("market_price") or 0.0),
            candle_index=int(market_payload.get("candle_index") or 0),
            parameters=dict(config_payload.get("parameters") or {}),
        )
        final_signal = str(material["final_signal"])
        final_reason = str(material["final_reason"])
        execution_intent = material["execution_intent"]
        strategy_specific_payload = {
            "policy_contract_version": CANARY_NON_SMA_POLICY_CONTRACT_VERSION,
            "can_emit_order_intent": True,
            "live_real_order_allowed": False,
            "parameters": dict(material["resolved"]),
        }
        return StrategyDecisionV2(
            strategy_name=CANARY_NON_SMA_STRATEGY_NAME,
            raw_signal=final_signal,
            raw_reason=final_reason,
            entry_signal=final_signal if final_signal == "BUY" else "HOLD",
            entry_reason=final_reason,
            exit_signal="HOLD",
            exit_reason=final_reason,
            final_signal=final_signal,
            final_reason=final_reason,
            blocked_filters=(),
            entry_blocked=False,
            entry_block_reason=None,
            exit_rule=None,
            exit_evaluations=(),
            protective_exit_overrode_entry=False,
            exit_filter_suppression_prevented=False,
            position_snapshot=PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False),
            execution_intent=execution_intent,
            entry_decision=object(),  # type: ignore[arg-type]
            trace={
                "strategy_name": CANARY_NON_SMA_STRATEGY_NAME,
                "final_signal": final_signal,
                "final_reason": final_reason,
                "strategy_specific_payload": dict(strategy_specific_payload),
            },
            policy_hash=str(material["policy_hash"]),
            policy_contract_hash=str(material["policy_contract_hash"]),
            policy_input_hash=str(material["policy_input_hash"]),
            policy_decision_hash=str(material["policy_decision_hash"]),
        )


def _evaluate_canary_result(
    *,
    pair: str,
    interval: str,
    candle_ts: int,
    market_price: float,
    candle_index: int,
    parameters: dict[str, Any],
    request: Any | None,
    evaluation_mode: str,
    feature_snapshot: RuntimeFeatureSnapshot | None = None,
) -> CanaryNonSmaRuntimeDecisionResult:
    material = _canary_policy_material(
        pair=pair,
        interval=interval,
        candle_ts=candle_ts,
        market_price=market_price,
        candle_index=candle_index,
        parameters=parameters,
    )
    resolved = dict(material["resolved"])
    final_signal = str(material["final_signal"])
    final_reason = str(material["final_reason"])
    execution_intent = material["execution_intent"]
    policy_hash = str(material["policy_hash"])
    policy_contract_hash = str(material["policy_contract_hash"])
    policy_input_hash = str(material["policy_input_hash"])
    policy_decision_hash = str(material["policy_decision_hash"])
    replay_fingerprint = dict(material["replay_fingerprint"])
    request_fields = (
        request.observability_fields()
        if request is not None and hasattr(request, "observability_fields")
        else {}
    )
    if isinstance(request_fields, dict):
        request_replay_fields = {
            key: value
            for key, value in request_fields.items()
            if key
            in {
                "runtime_decision_request_hash",
                "strategy_instance_id",
                "strategy_parameters_hash",
                "approved_profile_hash",
                "runtime_contract_hash",
                "plugin_contract_hash",
                "through_ts_ms",
            }
        }
        replay_fingerprint.update(request_replay_fields)
    runtime_feature_snapshot = (
        feature_snapshot.as_dict()
        if feature_snapshot is not None
        else {
            "schema_version": 1,
            "pair": pair,
            "interval": interval,
            "through_ts_ms": request_fields.get("through_ts_ms"),
            "decision_candle_ts": int(candle_ts),
            "capabilities_present": ["candles"],
            "capabilities_missing": [],
            "coverage_by_capability": {},
            "source_tables_or_streams": [],
            "db_schema_fingerprint": "compatibility_direct_material",
            "source_schema_hash": "compatibility_direct_material",
            "provider_name": "compatibility_direct_material",
            "provider_version": "1",
            "provider_contract_hash": "compatibility_direct_material",
            "runtime_data_availability_report_hash": "compatibility_direct_material",
            "staleness_ms": None,
            "feature_payload": {
                "candle_ts": int(candle_ts),
                "market_price": float(market_price),
                "last_close": float(market_price),
                "candle_index": int(candle_index),
            },
        }
    )
    if "market_snapshot_hash" not in runtime_feature_snapshot:
        runtime_feature_snapshot["market_snapshot_hash"] = sha256_prefixed(
            {
                "pair": pair,
                "interval": interval,
                "decision_candle_ts": int(candle_ts),
                "market_price": float(market_price),
                "last_close": float(market_price),
            }
        )
    if "feature_snapshot_hash" not in runtime_feature_snapshot:
        runtime_feature_snapshot["feature_snapshot_hash"] = sha256_prefixed(runtime_feature_snapshot)
    replay_fingerprint.update(
        {
            "feature_snapshot_hash": runtime_feature_snapshot["feature_snapshot_hash"],
            "market_snapshot_hash": runtime_feature_snapshot["market_snapshot_hash"],
            "runtime_data_availability_report_hash": runtime_feature_snapshot.get(
                "runtime_data_availability_report_hash"
            ),
            "provider_contract_hash": runtime_feature_snapshot.get("provider_contract_hash"),
        }
    )
    boundary = {
        "schema_version": 1,
        "decision_boundary_phase": "StrategyDecisionService.evaluate",
        "typed_authority": "StrategyDecisionV2",
        "order_submission_possible": final_signal in {"BUY", "SELL"},
        "read_only_replay_safe": True,
        "runtime_feature_snapshot_hash": runtime_feature_snapshot["feature_snapshot_hash"],
    }
    feature_snapshot = {
        "candle_ts": int(candle_ts),
        "last_close": float(market_price),
        "feature_family": "canary_close_only",
    }
    strategy_specific_payload = {
        "policy_contract_version": CANARY_NON_SMA_POLICY_CONTRACT_VERSION,
        "can_emit_order_intent": True,
        "live_real_order_allowed": False,
        "parameters": dict(resolved),
    }
    provenance = {
        **request_fields,
        "decision_boundary": "StrategyDecisionService.evaluate",
        "snapshot_builder": "RuntimeDataProvider.RuntimeFeatureSnapshot",
        "runtime_feature_snapshot": dict(runtime_feature_snapshot),
        "feature_snapshot_hash": runtime_feature_snapshot["feature_snapshot_hash"],
        "market_snapshot_hash": runtime_feature_snapshot["market_snapshot_hash"],
        "runtime_data_availability_report_hash": runtime_feature_snapshot.get(
            "runtime_data_availability_report_hash"
        ),
        "provider_contract_hash": runtime_feature_snapshot.get("provider_contract_hash"),
        "replay_fingerprint": replay_fingerprint,
        "strategy_parameters_hash": request_fields.get("strategy_parameters_hash")
        or sha256_prefixed(dict(resolved)),
        "approved_profile_hash_unavailable_reason": "canary_direct_compatibility_call"
        if not request_fields.get("approved_profile_hash")
        else "",
        "plugin_contract_hash_unavailable_reason": "canary_direct_compatibility_call"
        if not request_fields.get("plugin_contract_hash")
        else "",
        "runtime_contract_hash_unavailable_reason": "canary_direct_compatibility_call"
        if not request_fields.get("runtime_contract_hash")
        else "",
        "runtime_decision_request_hash_unavailable_reason": "canary_direct_compatibility_call"
        if not request_fields.get("runtime_decision_request_hash")
        else "",
        "code_provenance": {
            "policy_module": "bithumb_bot.strategy_plugins.canary_non_sma",
            "policy_class": "CanaryNonSmaPolicy",
        },
    }
    result = StrategyDecisionService().evaluate(
        StrategyEvaluationRequest(
            strategy_name=CANARY_NON_SMA_STRATEGY_NAME,
            strategy_instance_id=(
                str(request_fields.get("strategy_instance_id") or CANARY_NON_SMA_STRATEGY_NAME)
            ),
            mode=evaluation_mode,
            strategy_policy=CanaryNonSmaPolicy(),
            market_snapshot={
                "pair": pair,
                "interval": interval,
                "candle_ts": int(candle_ts),
                "market_price": float(market_price),
                "candle_index": int(candle_index),
                "feature_snapshot_hash": runtime_feature_snapshot["feature_snapshot_hash"],
                "market_snapshot_hash": runtime_feature_snapshot["market_snapshot_hash"],
            },
            position_snapshot=PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False),
            strategy_config={"parameters": dict(parameters or {})},
            execution_constraints=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
            exit_policy_config=None,
            rule_sources={},
            approved_profile_hash=(
                str(request_fields.get("approved_profile_hash") or "") or None
            ),
            runtime_contract_hash=(
                str(request_fields.get("runtime_contract_hash") or "") or None
            ),
            plugin_contract_hash=(
                str(request_fields.get("plugin_contract_hash") or "") or None
            ),
            request_hash=(
                str(request_fields.get("runtime_decision_request_hash") or "") or None
            ),
            provenance=provenance,
        )
    )
    result.decision.trace["strategy_evaluation_provenance"] = dict(result.provenance)
    result.decision.trace["replay_fingerprint_hash"] = result.replay_fingerprint_hash
    replay_fingerprint = dict(result.replay_fingerprint)
    base_context = {
        "market_price": float(market_price),
        "last_close": float(market_price),
        "strategy": CANARY_NON_SMA_STRATEGY_NAME,
        "signal": final_signal,
        "reason": final_reason,
        "raw_signal": final_signal,
        "raw_reason": final_reason,
        "final_signal": final_signal,
        "final_reason": final_reason,
        "execution_intent": execution_intent.as_dict() if execution_intent is not None else None,
        "feature_snapshot": feature_snapshot,
        "runtime_feature_snapshot": dict(runtime_feature_snapshot),
        "feature_snapshot_hash": runtime_feature_snapshot["feature_snapshot_hash"],
        "market_snapshot_hash": runtime_feature_snapshot["market_snapshot_hash"],
        "runtime_data_availability_report_hash": runtime_feature_snapshot.get(
            "runtime_data_availability_report_hash"
        ),
        "provider_contract_hash": runtime_feature_snapshot.get("provider_contract_hash"),
        "strategy_specific_payload": strategy_specific_payload,
        "strategy_diagnostics": {
            "schema_version": 1,
            "strategy_specific_diagnostics": {
                CANARY_NON_SMA_STRATEGY_NAME: dict(strategy_specific_payload)
            },
        },
        "position_gate": {
            "entry_allowed": True,
            "exit_allowed": False,
            "dust_state": "flat",
            "effective_flat": True,
            "normalized_exposure_active": False,
        },
        "position_state": {"comparison_state": "flat_no_dust_no_position"},
        "fee_authority": {
            "bid_fee": 0.0,
            "ask_fee": 0.0,
            "fee_source": "canary_order_intent_contract",
            "degraded": False,
            "degraded_reason": "none",
        },
        "order_rules": {"canary_order_intent": True},
        "position_lot_interpretation_costs": {"strategy": CANARY_NON_SMA_STRATEGY_NAME},
        "observability_context_authoritative": 0,
        "non_authoritative_observability_payload": True,
        "boundary": dict(boundary),
        "replay_fingerprint": dict(replay_fingerprint),
        "strategy_evaluation_provenance": dict(result.provenance),
        **request_fields,
    }
    return CanaryNonSmaRuntimeDecisionResult(
        decision=result.decision,
        base_context=base_context,
        candle_ts=int(candle_ts),
        market_price=float(market_price),
        replay_fingerprint=replay_fingerprint,
        boundary=boundary,
        policy_hashes=RuntimeStrategyPolicyHashes(
            {
                "pure_policy_hash": policy_hash,
                "policy_contract_hash": policy_contract_hash,
                "policy_input_hash": policy_input_hash,
                "policy_decision_hash": policy_decision_hash,
            }
        ),
    )


@dataclass(frozen=True)
class CanaryNonSmaRuntimeDecisionAdapter:
    strategy_name: str = CANARY_NON_SMA_STRATEGY_NAME

    def decide(
        self,
        conn: Any,
        request: Any,
    ) -> Any | None:
        from bithumb_bot.runtime_strategy_set import RuntimeMarketScope, RuntimeStrategySet, RuntimeStrategySpec

        pair = str(getattr(request, "pair", "") or "").strip()
        interval = str(getattr(request, "interval", "") or "").strip()
        if not pair:
            raise ValueError("canary_runtime_request_pair_missing")
        if not interval:
            raise ValueError("canary_runtime_request_interval_missing")
        spec = RuntimeStrategySpec(
            strategy_name=CANARY_NON_SMA_STRATEGY_NAME,
            pair=pair,
            interval=interval,
            parameters=dict(getattr(request, "parameters", {}) or {}),
        )
        strategy_set = RuntimeStrategySet(
            strategies=(spec,),
            source="canary_compatibility_provider",
            market_scope=RuntimeMarketScope(pair=pair, interval=interval),
        )
        resolver = RuntimeDataRequirementResolver()
        provider = SQLiteRuntimeDataProvider(conn, resolver=resolver)
        report = provider.preflight(strategy_set, through_ts_ms=request.through_ts_ms)
        if not report.ok:
            return None
        requirements = resolver.resolve_for_strategy_set(strategy_set)
        feature_snapshot = provider.snapshot(request, requirements)
        if feature_snapshot is None:
            return None
        return self.decide_feature_snapshot(request, feature_snapshot)

    def decide_feature_snapshot(
        self,
        request: Any,
        feature_snapshot: RuntimeFeatureSnapshot,
    ) -> Any | None:
        payload = feature_snapshot.feature_payload
        candle_ts = int(payload.get("candle_ts") or 0)
        if candle_ts <= 0:
            return None
        market_price = float(payload.get("market_price") or payload.get("last_close") or 0.0)
        candle_index = int(payload.get("candle_index") or 0)
        pair = str(getattr(request, "pair", "") or feature_snapshot.payload.get("pair") or "").strip()
        interval = str(getattr(request, "interval", "") or feature_snapshot.payload.get("interval") or "").strip()
        return _canary_result(
            pair=pair,
            interval=interval,
            candle_ts=candle_ts,
            market_price=market_price,
            candle_index=candle_index,
            parameters=dict(getattr(request, "parameters", {}) or {}),
            request=request,
            feature_snapshot=feature_snapshot,
        )

    def typed_authority_required(self) -> bool:
        return True


@dataclass(frozen=True)
class CanaryNonSmaRuntimeReplayStrategy:
    name: str = CANARY_NON_SMA_STRATEGY_NAME
    pair: str = ""
    interval: str = ""
    parameters: dict[str, Any] | None = None
    include_hold_execution_context_in_replay: bool = True

    def decide_runtime_snapshot(
        self,
        conn: Any,
        *,
        through_ts_ms: int | None = None,
    ) -> Any | None:
        from bithumb_bot.runtime_strategy_set import RuntimeDecisionRequestBuilder, RuntimeStrategySpec

        request = RuntimeDecisionRequestBuilder().build_for_spec(
            RuntimeStrategySpec(
                strategy_name=CANARY_NON_SMA_STRATEGY_NAME,
                pair=self.pair or None,
                interval=self.interval or None,
                parameters=dict(self.parameters or {}),
                parameter_source="approved_profile_strategy_parameters",
            ),
            through_ts_ms=through_ts_ms,
        )
        return CanaryNonSmaRuntimeDecisionAdapter().decide(
            conn,
            request,
        )

    def build_replay_bundle(
        self,
        conn: Any,
        *,
        through_ts_ms: int,
        readiness_payload: dict[str, object] | None = None,
    ) -> dict[str, Any] | None:
        before_total_changes = int(getattr(conn, "total_changes", 0))
        result = self.decide_runtime_snapshot(conn, through_ts_ms=int(through_ts_ms))
        after_total_changes = int(getattr(conn, "total_changes", 0))
        if result is None:
            return None
        from bithumb_bot.canonical_decision import build_runtime_replay_execution_plan_bundle

        execution_bundle = build_runtime_replay_execution_plan_bundle(
            conn,
            result,
            readiness_payload=readiness_payload,
        )
        return {
            "schema_version": 1,
            "strategy": self.name,
            "through_ts_ms": int(through_ts_ms),
            "boundary": dict(result.boundary),
            "read_only_replay": True,
            "post_decision_total_changes_delta": after_total_changes - before_total_changes,
            "policy_hashes": result.policy_hashes.as_dict(),
            "replay_fingerprint": dict(result.replay_fingerprint),
            "replay_fingerprint_hash": canonical_payload_hash(result.replay_fingerprint),
            "final_typed_strategy_decision": result.decision.as_trace(),
            "execution_decision_summary": (
                None if execution_bundle.summary is None else execution_bundle.summary.as_dict()
            ),
        }


def _build_canary_runtime_replay_strategy(
    profile: dict[str, Any],
    candidate_regime_policy: dict[str, Any] | None = None,
) -> CanaryNonSmaRuntimeReplayStrategy:
    del candidate_regime_policy
    params = profile.get("strategy_parameters") if isinstance(profile.get("strategy_parameters"), dict) else {}
    return CanaryNonSmaRuntimeReplayStrategy(
        pair=str(profile.get("market") or ""),
        interval=str(profile.get("interval") or ""),
        parameters=_normalize_canary_parameters(dict(params)),
    )


def _canary_single_replay_bundle_builder(
    conn: Any,
    strategy: Any,
    through_ts_ms: int,
    readiness_payload: dict[str, object] | None,
) -> dict[str, Any] | None:
    if not hasattr(strategy, "build_replay_bundle"):
        raise ValueError("canary_runtime_replay_strategy_missing_bundle_builder")
    return strategy.build_replay_bundle(
        conn,
        through_ts_ms=int(through_ts_ms),
        readiness_payload=readiness_payload,
    )


def _canary_runtime_parameters_from_env(env: dict[str, str]) -> dict[str, Any]:
    return _normalize_canary_parameters(
        {
            "CANARY_ORDER_START_INDEX": env.get("CANARY_ORDER_START_INDEX"),
            "CANARY_ORDER_SIDE": env.get("CANARY_ORDER_SIDE"),
            "CANARY_ORDER_REASON": env.get("CANARY_ORDER_REASON"),
            "CANARY_DECISION_START_INDEX": env.get("CANARY_DECISION_START_INDEX"),
            "CANARY_REASON": env.get("CANARY_REASON"),
        }
    )


def _canary_runtime_parameters_from_settings(_cfg: object) -> dict[str, Any]:
    return _normalize_canary_parameters({})


@dataclass(frozen=True)
class CanaryNonSmaPolicyAssembly:
    strategy_name: str = CANARY_NON_SMA_STRATEGY_NAME
    decision_contract_version: str = CANARY_NON_SMA_POLICY_CONTRACT_VERSION

    def materialize_parameters(self, raw: dict[str, Any]) -> dict[str, Any]:
        return _normalize_canary_parameters(dict(raw or {}))

    def replay_fingerprint_material(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "strategy_name": self.strategy_name,
            "decision_contract_version": self.decision_contract_version,
        }


def _canary_policy_assembly_factory() -> CanaryNonSmaPolicyAssembly:
    return CanaryNonSmaPolicyAssembly()


def build_canary_non_sma_research_events(
    *,
    dataset: Any,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    execution_timing_policy: Any,
    portfolio_policy: Any | None = None,
    context: Any | None = None,
) -> tuple[Any, ...]:
    del fee_rate, slippage_bps, portfolio_policy, context
    from bithumb_bot.research.decision_event import ResearchDecisionEvent
    from bithumb_bot.research.execution_timing import candle_close_ts
    from bithumb_bot.research.experiment_manifest import ExecutionTimingPolicy

    canary_parameters = _normalize_canary_parameters(parameter_values)
    start_index = max(0, int(canary_parameters["CANARY_ORDER_START_INDEX"]))
    timing_policy = execution_timing_policy or ExecutionTimingPolicy()
    events: list[ResearchDecisionEvent] = []
    for index, candle in enumerate(dataset.candles):
        if index < start_index:
            continue
        decision_ts = candle_close_ts(candle, interval=dataset.interval) + int(timing_policy.decision_guard_ms)
        feature_snapshot = {
            "candle_index": int(index),
            "close": float(candle.close),
            "feature_family": "canary_close_only",
        }
        material = _canary_policy_material(
            pair=str(dataset.market),
            interval=str(dataset.interval),
            candle_ts=int(candle.ts),
            market_price=float(candle.close),
            candle_index=int(index),
            parameters=dict(canary_parameters),
        )
        action = str(material["final_signal"])
        final_reason = str(material["final_reason"])
        strategy_specific_payload = {
            "policy_contract_version": CANARY_NON_SMA_POLICY_CONTRACT_VERSION,
            "can_emit_order_intent": True,
            "live_real_order_allowed": False,
            "parameters": dict(canary_parameters),
        }
        events.append(
            ResearchDecisionEvent(
                candle_ts=int(candle.ts),
                decision_ts=int(decision_ts),
                strategy_name=CANARY_NON_SMA_STRATEGY_NAME,
                strategy_version=CANARY_NON_SMA_SPEC.strategy_version,
                raw_signal=action,
                final_signal=action,
                reason=final_reason,
                feature_snapshot=feature_snapshot,
                strategy_diagnostics={
                    "schema_version": 1,
                    "strategy_specific_diagnostics": {
                        CANARY_NON_SMA_STRATEGY_NAME: dict(strategy_specific_payload)
                    },
                },
                entry_signal=action if action == "BUY" else "HOLD",
                exit_signal=action if action == "SELL" else "HOLD",
                order_intent=(
                    {
                        "side": "BUY",
                        "intent": "enter_strategy_position",
                        "requires_execution_sizing": True,
                    }
                    if action == "BUY"
                    else None
                ),
                extra_payload={
                    "strategy_specific_payload": strategy_specific_payload,
                    "pure_policy_hash": str(material["policy_hash"]),
                    "policy_contract_hash": str(material["policy_contract_hash"]),
                    "policy_input_hash": str(material["policy_input_hash"]),
                    "policy_decision_hash": str(material["policy_decision_hash"]),
                    "replay_fingerprint": dict(material["replay_fingerprint"]),
                    "replay_fingerprint_hash": str(material["replay_fingerprint_hash"]),
                },
            )
        )
    return tuple(events)


def _canary_research_policy_decision_builder(
    *,
    event: Any,
    dataset: Any,
    candle_index: int,
    position: Any,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    active_exit_policy: dict[str, Any],
    buy_fraction: float = 0.0,
    **_kwargs: Any,
) -> StrategyDecisionV2:
    del position, fee_rate, slippage_bps, active_exit_policy, buy_fraction
    candle = dataset.candles[int(candle_index)]
    return _canary_result(
        pair=str(dataset.market),
        interval=str(dataset.interval),
        candle_ts=int(event.candle_ts),
        market_price=float(candle.close),
        candle_index=int(candle_index),
        parameters=dict(parameter_values or {}),
        evaluation_mode="research_promotion",
    ).decision


def run_canary_non_sma_backtest(
    dataset: Any,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    parameter_stability_score: float | None = None,
    execution_model: Any | None = None,
    execution_timing_policy: Any | None = None,
    portfolio_policy: Any | None = None,
    context: Any | None = None,
) -> Any:
    from bithumb_bot.research.backtest_runner import run_plugin_backtest

    return run_plugin_backtest(
        plugin=CANARY_NON_SMA_PLUGIN,
        dataset=dataset,
        parameter_values=parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
        parameter_stability_score=parameter_stability_score,
        execution_model=execution_model,
        execution_timing_policy=execution_timing_policy,
        portfolio_policy=portfolio_policy,
        context=context,
    )


def _canary_decision_payload_adapter(
    payload: dict[str, object],
    event: Any,
) -> dict[str, object]:
    extra = event.extra_payload if isinstance(getattr(event, "extra_payload", None), dict) else {}
    strategy_specific_payload = (
        dict(extra.get("strategy_specific_payload"))
        if isinstance(extra.get("strategy_specific_payload"), dict)
        else {
            "policy_contract_version": CANARY_NON_SMA_POLICY_CONTRACT_VERSION,
            "can_emit_order_intent": True,
            "live_real_order_allowed": False,
        }
    )
    payload["strategy_specific_payload"] = strategy_specific_payload
    payload["policy_contract_hash"] = str(extra.get("policy_contract_hash") or "")
    payload["policy_input_hash"] = str(extra.get("policy_input_hash") or "")
    payload["policy_decision_hash"] = str(extra.get("policy_decision_hash") or "")
    payload["pure_policy_hash"] = str(extra.get("pure_policy_hash") or "")
    if not payload["pure_policy_hash"]:
        # Legacy adapter fallback for historical research events only; new
        # canary events carry the common StrategyDecisionEvidenceBuilder hash.
        payload["pure_policy_hash"] = sha256_prefixed(
            {
                "policy_contract_hash": payload["policy_contract_hash"],
                "policy_input_hash": payload["policy_input_hash"],
                "policy_decision_hash": payload["policy_decision_hash"],
            }
        )
    payload["replay_fingerprint"] = (
        dict(extra.get("replay_fingerprint"))
        if isinstance(extra.get("replay_fingerprint"), dict)
        else {}
    )
    payload["replay_fingerprint_hash"] = str(
        extra.get("replay_fingerprint_hash")
        or canonical_payload_hash(payload["replay_fingerprint"])
    )
    payload["market_regime"] = "not_evaluated"
    payload["regime_decision"] = "NOT_REQUIRED"
    payload["regime_block_reason"] = "none"
    return payload


_CANARY_NON_SMA_PROMOTION_EXTENSION = PromotionGradeStrategyExtension(
    runtime_replay_builder=_build_canary_runtime_replay_strategy,
    runtime_parameter_adapter=RuntimeParameterAdapter(
        from_env=_canary_runtime_parameters_from_env,
        from_settings=_canary_runtime_parameters_from_settings,
        env_keys=("CANARY_ORDER_START_INDEX", "CANARY_ORDER_SIDE", "CANARY_ORDER_REASON"),
    ),
    runtime_decision_adapter_factory=CanaryNonSmaRuntimeDecisionAdapter,
    policy_assembly_factory=_canary_policy_assembly_factory,
    decision_payload_adapter=_canary_decision_payload_adapter,
    research_policy_decision_builder=_canary_research_policy_decision_builder,
    single_replay_bundle_builder=_canary_single_replay_bundle_builder,
    live_dry_run_allowed=True,
    live_real_order_allowed=False,
    approved_profile_required=True,
    fail_closed_reason="canary_non_sma_live_real_order_not_allowed",
)


_CANARY_NON_SMA_RESEARCH_PLUGIN = research_plugin_from_event_builder(
    strategy_name=CANARY_NON_SMA_SPEC.strategy_name,
    version=CANARY_NON_SMA_SPEC.strategy_version,
    spec=CANARY_NON_SMA_SPEC,
    required_data=CANARY_NON_SMA_SPEC.required_data,
    optional_data=CANARY_NON_SMA_SPEC.optional_data,
    build_research_events=build_canary_non_sma_research_events,
    diagnostics_namespace=CANARY_NON_SMA_STRATEGY_NAME,
)


CANARY_NON_SMA_PLUGIN = build_live_eligible_strategy_plugin(
    research=_CANARY_NON_SMA_RESEARCH_PLUGIN,
    extension=_CANARY_NON_SMA_PROMOTION_EXTENSION,
    runner=run_canary_non_sma_backtest,
).to_research_strategy_plugin()
