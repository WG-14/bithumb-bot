from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .dataset_snapshot import DatasetSnapshot
from bithumb_bot.strategy_plugins.sma_with_filter_assembly import (
    MaterializationMode,
    SmaWithFilterPolicyAssembly,
)
from bithumb_bot.strategy_plugins.sma_with_filter_projector import (
    SmaWithFilterSnapshotProjector,
)
from bithumb_bot.strategy_plugins.sma_with_filter_contract import SMA_DECISION_EVIDENCE_CONTRACT
from bithumb_bot.strategy_decision_service import StrategyDecisionService, StrategyEvaluationRequest
from bithumb_bot.research.strategy_spec import materialized_strategy_parameters_hash


@dataclass(frozen=True)
class RuntimeReplayStrategyAdapter:
    strategy: Any
    runtime_decision_builder: Callable[..., Any]
    include_hold_execution_context_in_replay: bool = False

    @property
    def name(self) -> str:
        return str(getattr(self.strategy, "name", ""))

    def __getattr__(self, name: str) -> Any:
        return getattr(self.strategy, name)

    def decide_runtime_snapshot(
        self,
        conn: Any,
        *,
        through_ts_ms: int | None = None,
    ) -> Any:
        return self.runtime_decision_builder(
            conn,
            self.strategy,
            through_ts_ms=through_ts_ms,
        )


def build_runtime_replay_strategy(
    profile: dict[str, Any],
    candidate_regime_policy: dict[str, Any] | None = None,
) -> Any:
    assembly = SmaWithFilterPolicyAssembly()
    params = profile.get("strategy_parameters") if isinstance(profile.get("strategy_parameters"), dict) else {}
    try:
        materialized = assembly.materialize_parameters(
            dict(params),
            MaterializationMode.RUNTIME_REPLAY,
            profile=profile,
        )
    except Exception as exc:
        message = str(exc)
        if "runtime_bound_parameter_missing:" in message:
            missing = message.split("runtime_bound_parameter_missing:", 1)[1]
            raise RuntimeError("sma_runtime_request_behavior_parameter_missing:" + missing) from exc
        raise
    strategy = assembly.build_strategy(
        materialized,
        pair=str(profile.get("market") or ""),
        interval=str(profile.get("interval") or ""),
        candidate_regime_policy=candidate_regime_policy,
    )
    from bithumb_bot.runtime_sma_snapshot import decide_sma_with_filter_runtime_snapshot_from_db

    return RuntimeReplayStrategyAdapter(
        strategy=strategy,
        runtime_decision_builder=decide_sma_with_filter_runtime_snapshot_from_db,
    )


def runtime_parameters_from_env(env: dict[str, str]) -> dict[str, Any]:
    def _value(*keys: str, default: str = "") -> str:
        for key in keys:
            if env.get(key, "").strip() != "":
                return env[key]
        return default

    return {
        "SMA_SHORT": _value("SMA_SHORT", default="7"),
        "SMA_LONG": _value("SMA_LONG", default="30"),
        "SMA_FILTER_GAP_MIN_RATIO": _value("SMA_FILTER_GAP_MIN_RATIO", default="0.0012"),
        "SMA_FILTER_VOL_WINDOW": _value("SMA_FILTER_VOL_WINDOW", default="10"),
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": _value("SMA_FILTER_VOL_MIN_RANGE_RATIO", default="0.003"),
        "SMA_FILTER_OVEREXT_LOOKBACK": _value("SMA_FILTER_OVEREXT_LOOKBACK", default="3"),
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": _value("SMA_FILTER_OVEREXT_MAX_RETURN_RATIO", default="0.02"),
        "SMA_MARKET_REGIME_ENABLED": _value("SMA_MARKET_REGIME_ENABLED", default="true"),
        "SMA_COST_EDGE_ENABLED": _value("SMA_COST_EDGE_ENABLED", default="true"),
        "SMA_COST_EDGE_MIN_RATIO": _value(
            "SMA_COST_EDGE_MIN_RATIO",
            "STRATEGY_MIN_EXPECTED_EDGE_RATIO",
            default="0",
        ),
        "ENTRY_EDGE_BUFFER_RATIO": _value("ENTRY_EDGE_BUFFER_RATIO", default="0.0005"),
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO": _value("STRATEGY_MIN_EXPECTED_EDGE_RATIO", default="0"),
        "STRATEGY_ENTRY_SLIPPAGE_BPS": _value(
            "STRATEGY_ENTRY_SLIPPAGE_BPS",
            "MAX_MARKET_SLIPPAGE_BPS",
            "SLIPPAGE_BPS",
            default="0",
        ),
        "LIVE_FEE_RATE_ESTIMATE": _value(
            "LIVE_FEE_RATE_ESTIMATE",
            "PAPER_FEE_RATE",
            "PAPER_FEE_RATE_ESTIMATE",
            "FEE_RATE",
            default="0.0004",
        ),
        "STRATEGY_EXIT_RULES": _value("STRATEGY_EXIT_RULES", default="stop_loss,opposite_cross,max_holding_time"),
        "STRATEGY_EXIT_STOP_LOSS_RATIO": _value("STRATEGY_EXIT_STOP_LOSS_RATIO", default="0"),
        "STRATEGY_EXIT_MAX_HOLDING_MIN": _value("STRATEGY_EXIT_MAX_HOLDING_MIN", default="0"),
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": _value("STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO", default="0"),
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": _value(
            "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO",
            default="0",
        ),
    }


def runtime_parameters_from_settings(cfg: object) -> dict[str, Any]:
    from bithumb_bot.strategy_config import _sma_default, _sma_int

    return {
        "SMA_SHORT": int(getattr(cfg, "SMA_SHORT", _sma_int("SMA_SHORT"))),
        "SMA_LONG": int(getattr(cfg, "SMA_LONG", _sma_int("SMA_LONG"))),
        "SMA_FILTER_GAP_MIN_RATIO": float(
            getattr(cfg, "SMA_FILTER_GAP_MIN_RATIO", _sma_default("SMA_FILTER_GAP_MIN_RATIO"))
        ),
        "SMA_FILTER_VOL_WINDOW": int(
            getattr(cfg, "SMA_FILTER_VOL_WINDOW", _sma_default("SMA_FILTER_VOL_WINDOW"))
        ),
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": float(
            getattr(cfg, "SMA_FILTER_VOL_MIN_RANGE_RATIO", _sma_default("SMA_FILTER_VOL_MIN_RANGE_RATIO"))
        ),
        "SMA_FILTER_OVEREXT_LOOKBACK": int(
            getattr(cfg, "SMA_FILTER_OVEREXT_LOOKBACK", _sma_default("SMA_FILTER_OVEREXT_LOOKBACK"))
        ),
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": float(
            getattr(
                cfg,
                "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO",
                _sma_default("SMA_FILTER_OVEREXT_MAX_RETURN_RATIO"),
            )
        ),
        "SMA_MARKET_REGIME_ENABLED": bool(
            getattr(cfg, "SMA_MARKET_REGIME_ENABLED", _sma_default("SMA_MARKET_REGIME_ENABLED"))
        ),
        "SMA_COST_EDGE_ENABLED": bool(
            getattr(cfg, "SMA_COST_EDGE_ENABLED", _sma_default("SMA_COST_EDGE_ENABLED"))
        ),
        "SMA_COST_EDGE_MIN_RATIO": float(
            getattr(cfg, "SMA_COST_EDGE_MIN_RATIO", _sma_default("SMA_COST_EDGE_MIN_RATIO"))
        ),
        "ENTRY_EDGE_BUFFER_RATIO": float(getattr(cfg, "ENTRY_EDGE_BUFFER_RATIO")),
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO": float(getattr(cfg, "STRATEGY_MIN_EXPECTED_EDGE_RATIO")),
        "STRATEGY_ENTRY_SLIPPAGE_BPS": float(getattr(cfg, "STRATEGY_ENTRY_SLIPPAGE_BPS")),
        "LIVE_FEE_RATE_ESTIMATE": float(getattr(cfg, "LIVE_FEE_RATE_ESTIMATE")),
        "STRATEGY_EXIT_RULES": str(getattr(cfg, "STRATEGY_EXIT_RULES")),
        "STRATEGY_EXIT_STOP_LOSS_RATIO": float(getattr(cfg, "STRATEGY_EXIT_STOP_LOSS_RATIO")),
        "STRATEGY_EXIT_MAX_HOLDING_MIN": int(getattr(cfg, "STRATEGY_EXIT_MAX_HOLDING_MIN")),
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": float(getattr(cfg, "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO")),
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": float(
            getattr(cfg, "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO")
        ),
    }


def decision_payload_adapter(
    payload: dict[str, object],
    decision_or_artifact: Any,
) -> dict[str, object]:
    from bithumb_bot.canonical_decision import canonical_payload_hash
    from bithumb_bot.strategy_policy_contract import StrategyDecisionV2

    if isinstance(decision_or_artifact, StrategyDecisionV2):
        decision = decision_or_artifact
        trace = decision.as_trace()
        market_trace = trace.get("market") if isinstance(trace.get("market"), dict) else {}
        entry = decision.entry_decision
        prev_s = float(market_trace.get("prev_s", 0.0) or 0.0)
        prev_l = float(market_trace.get("prev_l", 0.0) or 0.0)
        curr_s = float(market_trace.get("curr_s", 0.0) or 0.0)
        curr_l = float(market_trace.get("curr_l", 0.0) or 0.0)
        gap_ratio = float(entry.gap_ratio)
        range_ratio = float(entry.volatility_ratio)
        required_edge_ratio = float(entry.edge_filter_details.get("required_edge_ratio", 0.0))
        authority = "StrategyDecisionV2"
    elif isinstance(decision_or_artifact, dict) and "pure_policy_trace" in decision_or_artifact:
        trace = decision_or_artifact.get("pure_policy_trace")
        trace_payload = dict(trace) if isinstance(trace, dict) else {}
        prev_s = float(trace_payload.get("prev_s", 0.0) or 0.0)
        prev_l = float(trace_payload.get("prev_l", 0.0) or 0.0)
        curr_s = float(trace_payload.get("curr_s", 0.0) or 0.0)
        curr_l = float(trace_payload.get("curr_l", 0.0) or 0.0)
        gap_ratio = float(decision_or_artifact.get("gap_ratio", 0.0) or 0.0)
        range_ratio = float(decision_or_artifact.get("volatility_ratio", 0.0) or 0.0)
        required_edge_ratio = float(decision_or_artifact.get("required_edge_ratio", 0.0) or 0.0)
        authority = "canonical_decision_artifact"
    else:
        raise TypeError("sma_decision_payload_adapter_requires_typed_decision_artifact")
    payload.update(
        {
            "prev_s": prev_s,
            "prev_l": prev_l,
            "curr_s": curr_s,
            "curr_l": curr_l,
            "gap_ratio": gap_ratio,
            "range_ratio": range_ratio,
            "expected_edge_ratio": gap_ratio,
            "required_edge_ratio": required_edge_ratio,
            "decision_payload_adapter_authority": authority,
            "decision_payload_adapter_contract": "typed_decision_artifact_only",
            "feature_hash": canonical_payload_hash(
                {
                    "prev_s": prev_s,
                    "prev_l": prev_l,
                    "curr_s": curr_s,
                    "curr_l": curr_l,
                    "gap_ratio": gap_ratio,
                    "range_ratio": range_ratio,
                }
            ),
        }
    )
    payload["strategy_diagnostic_count_defaults"] = _diagnostic_count_defaults()
    payload["strategy_diagnostic_counts"] = _diagnostic_counts(payload)
    return payload


def exit_signal_context(event: Any) -> dict[str, object]:
    # Compatibility-only helper for exploratory risk-gate paths. Promotion-grade
    # exit evaluation is owned by StrategyDecisionV2 trace/final assembly.
    event_extra = event.extra_payload if isinstance(getattr(event, "extra_payload", None), dict) else {}
    feature_snapshot = (
        event.feature_snapshot if isinstance(getattr(event, "feature_snapshot", None), dict) else {}
    )
    return {
        "curr_s": float(event_extra.get("curr_s", feature_snapshot.get("short_sma", 0.0)) or 0.0),
        "curr_l": float(event_extra.get("curr_l", feature_snapshot.get("long_sma", 0.0)) or 0.0),
    }


def research_export_normalizer(
    raw_decisions: list[dict[str, object]],
    snapshot: object,
    params: dict[str, object],
    profile: dict[str, object],
    order_rules_hash: str,
) -> list[dict[str, object]]:
    from bithumb_bot.research.decision_export_normalizers import sma_promotion_grade_research_export_decisions

    return sma_promotion_grade_research_export_decisions(
        raw_decisions=raw_decisions,
        snapshot=snapshot,
        params=params,
        profile=profile,
        order_rules_hash=order_rules_hash,
    )


def exit_rule_factory(
    active_exit_policy: dict[str, Any],
    parameter_values: dict[str, Any],
    fee_rate: float,
) -> list[Any]:
    # Compatibility-only exploratory factory. Promotion-grade paths use the
    # typed final decision assembler and do not re-evaluate strategy exits here.
    from bithumb_bot.strategy.exit_rules import create_sma_exit_rules

    return create_sma_exit_rules(
        rule_names=list(active_exit_policy.get("rules") or ()),
        stop_loss_ratio=float(active_exit_policy.get("stop_loss", {}).get("stop_loss_ratio", 0.0)),
        max_holding_sec=float(
            active_exit_policy.get("max_holding_time", {}).get("max_holding_min", 0.0)
        )
        * 60.0,
        min_take_profit_ratio=float(
            active_exit_policy.get("opposite_cross", {}).get("min_take_profit_ratio", 0.0)
        ),
        live_fee_rate_estimate=float(parameter_values.get("LIVE_FEE_RATE_ESTIMATE") or fee_rate),
        small_loss_tolerance_ratio=float(
            active_exit_policy.get("opposite_cross", {}).get("small_loss_tolerance_ratio", 0.0)
        ),
    )


def exit_policy_materializer(strategy_name: str, parameter_values: dict[str, Any]) -> dict[str, object]:
    return SmaWithFilterPolicyAssembly().materialize_exit_policy(
        strategy_name,
        parameter_values,
        materialization_mode=MaterializationMode.RESEARCH_PROMOTION.value,
    )


def research_policy_decision_builder(
    *,
    event: Any,
    dataset: DatasetSnapshot,
    candle_index: int,
    position: Any,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    active_exit_policy: dict[str, Any],
    buy_fraction: float = 0.0,
    materialization_mode: MaterializationMode | str = MaterializationMode.RESEARCH_EXPLORATORY,
    candidate_regime_policy: dict[str, object] | None = None,
    candidate_regime_policy_enforced: bool | None = None,
) -> Any:
    projector = SmaWithFilterSnapshotProjector(SmaWithFilterPolicyAssembly())
    projected = projector.project_from_research_event(
        event=event,
        dataset=dataset,
        candle_index=candle_index,
        position=position,
        parameter_values=parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
        active_exit_policy=active_exit_policy,
        buy_fraction=buy_fraction,
        materialization_mode=materialization_mode,
        candidate_regime_policy=candidate_regime_policy,
        candidate_regime_policy_enforced=candidate_regime_policy_enforced,
    )
    if projected is None:
        return None
    materialized = projected.materialized
    strategy = projected.strategy
    bundle = projected.bundle
    strategy_parameters_hash = materialized_strategy_parameters_hash(dict(materialized.values))
    approved_profile_hash = (
        candidate_regime_policy.get("strategy_profile_hash")
        if isinstance(candidate_regime_policy, dict)
        else None
    )
    result = StrategyDecisionService().evaluate(
        StrategyEvaluationRequest(
            strategy_name=strategy.name,
            strategy_instance_id=f"{strategy.name}:research_promotion",
            mode=str(
                materialization_mode.value
                if isinstance(materialization_mode, MaterializationMode)
                else materialization_mode
            ),
            strategy_policy=strategy,
            market_snapshot=bundle.market,
            position_snapshot=bundle.position,
            strategy_config=bundle.config,
            execution_constraints=bundle.execution_constraints,
            exit_policy_config=bundle.exit_policy_config,
            rule_sources=projected.rule_sources,
            approved_profile_hash=approved_profile_hash,
            runtime_contract_hash=None,
            plugin_contract_hash=None,
            request_hash=None,
            provenance={
                "decision_boundary": "StrategyDecisionService.evaluate",
                "snapshot_builder": "SmaWithFilterSnapshotProjector",
                "candle_ts": int(event.candle_ts),
                "strategy_parameters_hash": strategy_parameters_hash,
                "replay_fingerprint": projected.replay_fingerprint,
                "approved_profile_hash_unavailable_reason": "research_candidate_profile_not_supplied"
                if not approved_profile_hash
                else "",
                "plugin_contract_hash_unavailable_reason": "research_policy_decision_builder_no_plugin_contract",
                "runtime_contract_hash_unavailable_reason": "research_policy_decision_builder_no_runtime_contract",
                "runtime_decision_request_hash_unavailable_reason": "research_policy_decision_builder_no_runtime_request",
                "code_provenance": {
                    "policy_module": strategy.__class__.__module__,
                    "policy_class": strategy.__class__.__name__,
                },
            },
            decision_input_bundle=bundle,
            decision_evidence_contract=SMA_DECISION_EVIDENCE_CONTRACT,
        )
    )
    result.decision.trace["strategy_evaluation_provenance"] = dict(result.provenance)
    result.decision.trace["replay_fingerprint_hash"] = result.replay_fingerprint_hash
    result.decision.trace.update(bundle.observability_payload())
    result.decision.trace.update(
        {
            "parameter_sources": dict(materialized.sources),
            "runtime_comparable": bool(materialized.runtime_comparable),
            "materialization_mode": materialized.mode.value,
            "policy_materialization_mode": materialized.mode.value,
            "legacy_defaults_used": list(materialized.legacy_defaults_used),
        }
    )
    return result.decision


def runtime_decision_adapter_factory() -> Any:
    from bithumb_bot.runtime_adapters.sma_with_filter import SmaWithFilterRuntimeDecisionAdapter

    return SmaWithFilterRuntimeDecisionAdapter()


def runtime_feature_snapshot_builder(*, conn, request, feature_snapshot) -> Any:
    from bithumb_bot.runtime_adapters.sma_with_filter import (
        build_sma_with_filter_runtime_feature_snapshot,
    )

    return build_sma_with_filter_runtime_feature_snapshot(
        conn=conn,
        request=request,
        feature_snapshot=feature_snapshot,
    )


def policy_assembly_factory() -> SmaWithFilterPolicyAssembly:
    return SmaWithFilterPolicyAssembly()


def single_replay_bundle_builder(
    conn: Any,
    strategy: Any,
    through_ts_ms: int,
    readiness_payload: dict[str, object] | None,
) -> dict[str, Any] | None:
    from bithumb_bot.runtime_sma_snapshot import build_sma_with_filter_replay_bundle

    return build_sma_with_filter_replay_bundle(
        conn,
        strategy,
        through_ts_ms=int(through_ts_ms),
        readiness_payload=readiness_payload,
    )


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _diagnostic_count_defaults() -> dict[str, int]:
    return {
        "raw_sell_filter_blocked_while_in_position_count": 0,
        "raw_buy_filter_blocked_count": 0,
        "opposite_cross_triggered_count": 0,
        "opposite_cross_deferred_small_loss_count": 0,
        "opposite_cross_deferred_small_gain_count": 0,
        "stop_loss_exit_count": 0,
        "max_holding_exit_count": 0,
        "exit_filter_suppression_prevented_count": 0,
    }


def _diagnostic_counts(payload: dict[str, object]) -> dict[str, int]:
    counts: dict[str, int] = {}
    raw_signal = str(payload.get("raw_signal") or "").upper()
    raw_filter_would_block = bool(
        payload.get("raw_filter_would_block", payload.get("entry_filter_blocked"))
    )
    entry_blocked = bool(payload.get("entry_blocked"))
    sellable_qty = float(payload.get("sellable_qty") or 0.0)
    if raw_signal == "BUY" and entry_blocked:
        counts["raw_buy_filter_blocked_count"] = 1
    if raw_signal == "SELL" and raw_filter_would_block and sellable_qty > 1e-12:
        counts["raw_sell_filter_blocked_while_in_position_count"] = 1
    if bool(payload.get("exit_filter_suppression_prevented")):
        counts["exit_filter_suppression_prevented_count"] = 1
    for evaluation in payload.get("exit_evaluations") or []:
        if not isinstance(evaluation, dict):
            continue
        context = evaluation.get("context") if isinstance(evaluation.get("context"), dict) else {}
        rule = str(evaluation.get("rule") or context.get("rule") or "")
        if rule == "opposite_cross":
            if bool(context.get("opposite_cross_triggered")):
                counts["opposite_cross_triggered_count"] = counts.get("opposite_cross_triggered_count", 0) + 1
            if bool(context.get("filter_applied")):
                zone = str(context.get("filter_zone") or "")
                if zone == "small_loss":
                    counts["opposite_cross_deferred_small_loss_count"] = (
                        counts.get("opposite_cross_deferred_small_loss_count", 0) + 1
                    )
                elif zone == "small_gain":
                    counts["opposite_cross_deferred_small_gain_count"] = (
                        counts.get("opposite_cross_deferred_small_gain_count", 0) + 1
                    )
        elif rule == "stop_loss" and bool(evaluation.get("triggered")):
            counts["stop_loss_exit_count"] = counts.get("stop_loss_exit_count", 0) + 1
        elif rule == "max_holding_time" and bool(evaluation.get("triggered")):
            counts["max_holding_exit_count"] = counts.get("max_holding_exit_count", 0) + 1
    return counts
