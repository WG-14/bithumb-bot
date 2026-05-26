from __future__ import annotations

import sqlite3
from typing import Any

from .execution_service import build_execution_decision_summary
from .runtime_position_state_normalizer import PositionStateNormalizer
from .runtime_sma_snapshot_builder import (
    decide_sma_with_filter_snapshot_from_db as _runtime_snapshot_from_db,
    decide_sma_with_filter_runtime_snapshot_from_db as _runtime_typed_snapshot_from_db,
    RuntimeSmaDecisionResult,
)
from .strategy.base import StrategyDecision
from .strategy.sma_policy_strategy import SmaWithFilterStrategy

SMA_RUNTIME_BOUNDARY_STAGES = {
    "snapshot_builder": "runtime_sma_snapshot.decide_sma_with_filter_snapshot_from_db",
    "pure_policy": "core.sma_policy.evaluate_sma_policy",
    "final_decision_assembler": "strategy.sma_decision_assembler.evaluate_sma_final_decision",
    "execution_planner": "execution_service.build_execution_decision_summary",
    "broker_submit_path": "engine.submit_or_suppress",
}


def _code_provenance() -> dict[str, object]:
    return {"source": "unavailable"}


def _typed_strategy_decision_payload(result: RuntimeSmaDecisionResult) -> dict[str, object]:
    decision = result.decision
    return {
        "strategy_name": decision.strategy_name,
        "raw_signal": decision.raw_signal,
        "raw_reason": decision.raw_reason,
        "entry_signal": decision.entry_signal,
        "entry_reason": decision.entry_reason,
        "exit_signal": decision.exit_signal,
        "exit_reason": decision.exit_reason,
        "final_signal": decision.final_signal,
        "final_reason": decision.final_reason,
        "blocked_filters": list(decision.blocked_filters),
        "entry_blocked": bool(decision.entry_blocked),
        "entry_block_reason": decision.entry_block_reason,
        "exit_rule": decision.exit_rule,
        "exit_evaluations": [dict(item) for item in decision.exit_evaluations],
        "protective_exit_overrode_entry": bool(decision.protective_exit_overrode_entry),
        "exit_filter_suppression_prevented": bool(decision.exit_filter_suppression_prevented),
        "execution_intent": (
            decision.execution_intent.as_dict()
            if decision.execution_intent is not None
            else None
        ),
        "policy_hash": decision.policy_hash,
        "policy_contract_hash": decision.policy_contract_hash,
        "policy_input_hash": decision.policy_input_hash,
        "policy_decision_hash": decision.policy_decision_hash,
        "trace": decision.as_trace(),
    }


class ReadOnlyPositionStateNormalizer:
    """Replay/debug adapter that forbids persistence before snapshot loading."""

    def normalize_and_persist(self, conn: sqlite3.Connection, **kwargs: object) -> int:
        return 0


def decide_sma_with_filter_snapshot_from_db(
    conn: sqlite3.Connection,
    strategy: SmaWithFilterStrategy,
    *,
    through_ts_ms: int | None = None,
    normalizer: PositionStateNormalizer | None = None,
) -> StrategyDecision | None:
    """Runtime boundary for SMA DB state -> snapshots -> typed final decision.

    The legacy ``Strategy.decide(conn)`` facade remains available for older
    callers, but live/replay orchestration should bind here so the mutable DB
    normalization boundary is explicit and separately testable.
    """
    decision = _runtime_snapshot_from_db(
        conn,
        strategy,
        through_ts_ms=through_ts_ms,
        normalizer=normalizer,
    )
    return decision


def decide_sma_with_filter_runtime_snapshot_from_db(
    conn: sqlite3.Connection,
    strategy: SmaWithFilterStrategy,
    *,
    through_ts_ms: int | None = None,
    normalizer: PositionStateNormalizer | None = None,
) -> RuntimeSmaDecisionResult | None:
    """Typed runtime boundary for SMA DB state -> snapshots -> final decision."""
    return _runtime_typed_snapshot_from_db(
        conn,
        strategy,
        through_ts_ms=through_ts_ms,
        normalizer=normalizer,
    )


def build_sma_with_filter_replay_bundle(
    conn: sqlite3.Connection,
    strategy: SmaWithFilterStrategy,
    *,
    through_ts_ms: int,
    readiness_payload: dict[str, object] | None = None,
    previous_target_exposure_krw: float | None = None,
) -> dict[str, Any] | None:
    """Build structured read-only replay material for one SMA decision."""
    typed_result = decide_sma_with_filter_runtime_snapshot_from_db(
        conn,
        strategy,
        through_ts_ms=int(through_ts_ms),
        normalizer=ReadOnlyPositionStateNormalizer(),
    )
    if typed_result is None:
        return None
    decision = typed_result.legacy_strategy_decision()
    strategy_payload = decision.as_dict()
    context = dict(decision.context)
    execution_summary = build_execution_decision_summary(
        decision_context=context,
        readiness_payload=readiness_payload,
        raw_signal=strategy_payload.get("raw_signal"),
        final_signal=strategy_payload.get("final_signal", strategy_payload.get("signal")),
        final_reason=strategy_payload.get("reason"),
        previous_target_exposure_krw=previous_target_exposure_krw,
    )
    execution_decision_reconstructable = readiness_payload is not None
    execution_decision_reconstruction_reason = (
        "readiness_payload_supplied"
        if execution_decision_reconstructable
        else "live_readiness_context_not_available_in_db_snapshot"
    )
    pure_policy_trace = (
        dict(context.get("pure_policy_trace"))
        if isinstance(context.get("pure_policy_trace"), dict)
        else {}
    )
    return {
        "schema_version": 1,
        "decision_context_schema_version": 1,
        "strategy": strategy.name,
        "through_ts_ms": int(through_ts_ms),
        "boundary_stages": dict(SMA_RUNTIME_BOUNDARY_STAGES),
        "code_provenance": _code_provenance(),
        "market_snapshot": {
            "pair": context.get("pair"),
            "interval": context.get("interval"),
            "candle_ts": context.get("ts"),
            "last_close": context.get("last_close"),
            "features": context.get("features"),
        },
        "position_snapshot": context.get("position_state"),
        "policy_config": {
            "short_n": int(strategy.short_n),
            "long_n": int(strategy.long_n),
            "min_gap_ratio": float(strategy.min_gap_ratio),
            "volatility_window": int(strategy.volatility_window),
            "min_volatility_ratio": float(strategy.min_volatility_ratio),
            "overextended_lookback": int(strategy.overextended_lookback),
            "overextended_max_return_ratio": float(strategy.overextended_max_return_ratio),
            "cost_edge_enabled": bool(strategy.cost_edge_enabled),
            "cost_edge_min_ratio": float(strategy.cost_edge_min_ratio),
            "market_regime_enabled": bool(strategy.market_regime_enabled),
        },
        "execution_constraint_snapshot": {
            "fee_authority": context.get("fee_authority"),
            "order_rules": context.get("order_rules"),
        },
        "policy_input_hash": context.get("policy_input_hash"),
        "policy_decision_hash": context.get("policy_decision_hash"),
        "pure_policy_hash": context.get("pure_policy_hash"),
        "replay_fingerprint": context.get("replay_fingerprint"),
        "pure_policy_trace": pure_policy_trace,
        "final_strategy_decision": strategy_payload,
        "final_typed_strategy_decision": _typed_strategy_decision_payload(typed_result),
        "execution_decision_reconstructable": execution_decision_reconstructable,
        "execution_decision_reconstruction_reason": execution_decision_reconstruction_reason,
        "execution_decision_summary": execution_summary.as_dict(),
    }
