from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bithumb_bot.canonical_decision import canonical_payload_hash
from bithumb_bot.core.sma_policy import PositionSnapshot, StrategyDecisionV2
from bithumb_bot.decision_equivalence import sha256_prefixed
from bithumb_bot.research.strategy_registry import ResearchStrategyPlugin, RuntimeParameterAdapter
from bithumb_bot.research.strategy_spec import StrategySpec, materialize_strategy_parameters
from bithumb_bot.runtime_decision_contract import RuntimeStrategyPolicyHashes


CANARY_NON_SMA_STRATEGY_NAME = "canary_non_sma"
CANARY_NON_SMA_POLICY_CONTRACT_VERSION = "canary_non_sma.no_order_policy.v1"


def _settings() -> Any:
    from bithumb_bot.config import settings

    return settings


CANARY_NON_SMA_SPEC = StrategySpec(
    strategy_name=CANARY_NON_SMA_STRATEGY_NAME,
    strategy_version="canary_non_sma.promotion_contract.v1",
    accepted_parameter_names=("CANARY_DECISION_START_INDEX", "CANARY_REASON"),
    required_parameter_names=(),
    behavior_affecting_parameter_names=("CANARY_DECISION_START_INDEX", "CANARY_REASON"),
    metadata_only_parameter_names=(),
    research_only_parameter_names=(),
    default_parameters={
        "CANARY_DECISION_START_INDEX": 0,
        "CANARY_REASON": "canary_non_sma_no_order_contract",
    },
    decision_contract_version="research_canary_non_sma_decision_contract.v1",
    required_data=("candles",),
    optional_data=(),
    exit_policy_schema={
        "schema_version": 1,
        "rules": (),
        "no_order_capability_contract": {
            "can_submit_orders": False,
            "reason": "architecture canary proves promotion-grade replay without order authority",
        },
    },
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


def _latest_runtime_candle(conn: Any, *, through_ts_ms: int | None) -> tuple[int, float] | None:
    cfg = _settings()
    query = "SELECT ts, close FROM candles WHERE pair=? AND interval=?"
    params: list[object] = [cfg.PAIR, cfg.INTERVAL]
    if through_ts_ms is not None:
        query += " AND ts<=?"
        params.append(int(through_ts_ms))
    query += " ORDER BY ts DESC LIMIT 1"
    row = conn.execute(query, tuple(params)).fetchone()
    if row is None:
        return None
    candle_ts = int(row["ts"]) if hasattr(row, "keys") else int(row[0])
    close = float(row["close"]) if hasattr(row, "keys") else float(row[1])
    return candle_ts, close


def _canary_result(*, candle_ts: int, market_price: float) -> CanaryNonSmaRuntimeDecisionResult:
    cfg = _settings()
    policy_contract = {
        "schema_version": 1,
        "strategy_name": CANARY_NON_SMA_STRATEGY_NAME,
        "policy_contract_version": CANARY_NON_SMA_POLICY_CONTRACT_VERSION,
        "can_submit_orders": False,
    }
    policy_input = {
        "schema_version": 1,
        "strategy_name": CANARY_NON_SMA_STRATEGY_NAME,
        "pair": str(cfg.PAIR),
        "interval": str(cfg.INTERVAL),
        "candle_ts": int(candle_ts),
        "market_price": float(market_price),
    }
    policy_decision = {
        "schema_version": 1,
        "raw_signal": "HOLD",
        "final_signal": "HOLD",
        "final_reason": "canary_non_sma_no_order_contract",
        "execution_intent": None,
    }
    policy_hash = sha256_prefixed(
        {
            "policy_contract": policy_contract,
            "policy_input": policy_input,
            "policy_decision": policy_decision,
        }
    )
    policy_contract_hash = sha256_prefixed(policy_contract)
    policy_input_hash = sha256_prefixed(policy_input)
    policy_decision_hash = sha256_prefixed(policy_decision)
    replay_fingerprint = {
        "schema_version": 1,
        "strategy_name": CANARY_NON_SMA_STRATEGY_NAME,
        "policy_contract_version": CANARY_NON_SMA_POLICY_CONTRACT_VERSION,
        "policy_contract_hash": policy_contract_hash,
        "policy_input_hash": policy_input_hash,
        "policy_decision_hash": policy_decision_hash,
        "candle_ts": int(candle_ts),
        "market_price": float(market_price),
    }
    boundary = {
        "schema_version": 1,
        "decision_boundary_phase": "canary_non_sma_runtime_decision",
        "typed_authority": "StrategyDecisionV2",
        "order_submission_possible": False,
        "read_only_replay_safe": True,
    }
    feature_snapshot = {
        "candle_ts": int(candle_ts),
        "last_close": float(market_price),
        "feature_family": "canary_close_only",
    }
    strategy_specific_payload = {
        "policy_contract_version": CANARY_NON_SMA_POLICY_CONTRACT_VERSION,
        "can_submit_orders": False,
    }
    decision = StrategyDecisionV2(
        strategy_name=CANARY_NON_SMA_STRATEGY_NAME,
        raw_signal="HOLD",
        raw_reason="canary_non_sma_no_order_contract",
        entry_signal="HOLD",
        entry_reason="canary_non_sma_no_order_contract",
        exit_signal="HOLD",
        exit_reason="canary_non_sma_no_order_contract",
        final_signal="HOLD",
        final_reason="canary_non_sma_no_order_contract",
        blocked_filters=(),
        entry_blocked=False,
        entry_block_reason=None,
        exit_rule=None,
        exit_evaluations=(),
        protective_exit_overrode_entry=False,
        exit_filter_suppression_prevented=False,
        position_snapshot=PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False),
        execution_intent=None,
        entry_decision=object(),  # type: ignore[arg-type]
        trace={
            "strategy_name": CANARY_NON_SMA_STRATEGY_NAME,
            "final_signal": "HOLD",
            "final_reason": "canary_non_sma_no_order_contract",
            "strategy_specific_payload": dict(strategy_specific_payload),
        },
        policy_hash=policy_hash,
        policy_contract_hash=policy_contract_hash,
        policy_input_hash=policy_input_hash,
        policy_decision_hash=policy_decision_hash,
    )
    base_context = {
        "market_price": float(market_price),
        "last_close": float(market_price),
        "strategy": CANARY_NON_SMA_STRATEGY_NAME,
        "signal": "HOLD",
        "reason": "canary_non_sma_no_order_contract",
        "raw_signal": "HOLD",
        "raw_reason": "canary_non_sma_no_order_contract",
        "final_signal": "HOLD",
        "final_reason": "canary_non_sma_no_order_contract",
        "feature_snapshot": feature_snapshot,
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
            "fee_source": "canary_no_order_contract",
            "degraded": False,
            "degraded_reason": "none",
        },
        "order_rules": {"canary_no_order": True},
        "position_lot_interpretation_costs": {"strategy": CANARY_NON_SMA_STRATEGY_NAME},
        "observability_context_authoritative": 0,
        "non_authoritative_observability_payload": True,
        "boundary": dict(boundary),
        "replay_fingerprint": dict(replay_fingerprint),
    }
    return CanaryNonSmaRuntimeDecisionResult(
        decision=decision,
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
        *,
        short_n: int,
        long_n: int,
        through_ts_ms: int | None = None,
    ) -> Any | None:
        del short_n, long_n
        candle = _latest_runtime_candle(conn, through_ts_ms=through_ts_ms)
        if candle is None:
            return None
        candle_ts, market_price = candle
        return _canary_result(candle_ts=candle_ts, market_price=market_price)

    def typed_authority_required(self) -> bool:
        return True


@dataclass(frozen=True)
class CanaryNonSmaRuntimeReplayStrategy:
    name: str = CANARY_NON_SMA_STRATEGY_NAME
    include_hold_execution_context_in_replay: bool = True

    def decide_runtime_snapshot(
        self,
        conn: Any,
        *,
        through_ts_ms: int | None = None,
    ) -> Any | None:
        return CanaryNonSmaRuntimeDecisionAdapter().decide(
            conn,
            short_n=0,
            long_n=0,
            through_ts_ms=through_ts_ms,
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
    del profile, candidate_regime_policy
    return CanaryNonSmaRuntimeReplayStrategy()


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
    return {
        "CANARY_DECISION_START_INDEX": int(env.get("CANARY_DECISION_START_INDEX") or 0),
        "CANARY_REASON": env.get("CANARY_REASON") or "canary_non_sma_no_order_contract",
    }


def _canary_runtime_parameters_from_settings(_cfg: object) -> dict[str, Any]:
    return {
        "CANARY_DECISION_START_INDEX": 0,
        "CANARY_REASON": "canary_non_sma_no_order_contract",
    }


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
    del portfolio_policy
    from bithumb_bot.research.backtest_kernel import run_decision_event_backtest
    from bithumb_bot.research.decision_event import ResearchDecisionEvent
    from bithumb_bot.research.execution_timing import candle_close_ts
    from bithumb_bot.research.experiment_manifest import ExecutionTimingPolicy

    effective_parameters = materialize_strategy_parameters(
        CANARY_NON_SMA_STRATEGY_NAME,
        parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
    )
    start_index = max(0, int(effective_parameters.get("CANARY_DECISION_START_INDEX", 0)))
    reason = str(effective_parameters.get("CANARY_REASON") or "canary_non_sma_no_order_contract")
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
        strategy_specific_payload = {
            "policy_contract_version": CANARY_NON_SMA_POLICY_CONTRACT_VERSION,
            "can_submit_orders": False,
        }
        policy_contract_hash = sha256_prefixed(
            {
                "strategy_name": CANARY_NON_SMA_STRATEGY_NAME,
                "policy_contract_version": CANARY_NON_SMA_POLICY_CONTRACT_VERSION,
                "can_submit_orders": False,
            }
        )
        policy_input_hash = sha256_prefixed(feature_snapshot)
        policy_decision_hash = sha256_prefixed({"final_signal": "HOLD", "reason": reason})
        events.append(
            ResearchDecisionEvent(
                candle_ts=int(candle.ts),
                decision_ts=int(decision_ts),
                strategy_name=CANARY_NON_SMA_STRATEGY_NAME,
                strategy_version=CANARY_NON_SMA_SPEC.strategy_version,
                raw_signal="HOLD",
                final_signal="HOLD",
                reason=reason,
                feature_snapshot=feature_snapshot,
                strategy_diagnostics={
                    "schema_version": 1,
                    "strategy_specific_diagnostics": {
                        CANARY_NON_SMA_STRATEGY_NAME: dict(strategy_specific_payload)
                    },
                },
                entry_signal="HOLD",
                exit_signal="HOLD",
                extra_payload={
                    "strategy_specific_payload": strategy_specific_payload,
                    "policy_contract_hash": policy_contract_hash,
                    "policy_input_hash": policy_input_hash,
                    "policy_decision_hash": policy_decision_hash,
                    "replay_fingerprint": {
                        "strategy_name": CANARY_NON_SMA_STRATEGY_NAME,
                        "candle_ts": int(candle.ts),
                        "policy_contract_hash": policy_contract_hash,
                        "policy_input_hash": policy_input_hash,
                        "policy_decision_hash": policy_decision_hash,
                    },
                },
            )
        )
    return run_decision_event_backtest(
        dataset=dataset,
        strategy_name=CANARY_NON_SMA_STRATEGY_NAME,
        parameter_values=effective_parameters,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
        decision_events=tuple(events),
        parameter_stability_score=parameter_stability_score,
        execution_model=execution_model,
        execution_timing_policy=timing_policy,
        portfolio_policy=None,
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
            "can_submit_orders": False,
        }
    )
    payload["strategy_specific_payload"] = strategy_specific_payload
    payload["policy_contract_hash"] = str(extra.get("policy_contract_hash") or "")
    payload["policy_input_hash"] = str(extra.get("policy_input_hash") or "")
    payload["policy_decision_hash"] = str(extra.get("policy_decision_hash") or "")
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
    payload["replay_fingerprint_hash"] = canonical_payload_hash(payload["replay_fingerprint"])
    payload["market_regime"] = "not_evaluated"
    payload["regime_decision"] = "NOT_REQUIRED"
    payload["regime_block_reason"] = "none"
    return payload


CANARY_NON_SMA_PLUGIN = ResearchStrategyPlugin(
    name=CANARY_NON_SMA_SPEC.strategy_name,
    version=CANARY_NON_SMA_SPEC.strategy_version,
    spec=CANARY_NON_SMA_SPEC,
    required_data=CANARY_NON_SMA_SPEC.required_data,
    optional_data=CANARY_NON_SMA_SPEC.optional_data,
    runner=run_canary_non_sma_backtest,
    runtime_replay_builder=_build_canary_runtime_replay_strategy,
    runtime_parameter_adapter=RuntimeParameterAdapter(
        from_env=_canary_runtime_parameters_from_env,
        from_settings=_canary_runtime_parameters_from_settings,
        env_keys=("CANARY_DECISION_START_INDEX", "CANARY_REASON"),
    ),
    decision_contract_version=CANARY_NON_SMA_SPEC.decision_contract_version,
    diagnostics_namespace=CANARY_NON_SMA_STRATEGY_NAME,
    decision_payload_adapter=_canary_decision_payload_adapter,
    runtime_decision_adapter_factory=CanaryNonSmaRuntimeDecisionAdapter,
    single_replay_bundle_builder=_canary_single_replay_bundle_builder,
)
