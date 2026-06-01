from __future__ import annotations

import sqlite3
import subprocess
from pathlib import Path
from typing import Any

from .execution_service import (
    ExecutionReadinessPlanningInput,
    ExecutionTargetPlanningInput,
    TypedExecutionPlanningInput,
    build_typed_execution_decision_summary,
)
from . import runtime_sma_snapshot_builder
from .runtime_sma_snapshot_builder import RuntimeSmaDecisionResult
from .strategy.base import StrategyDecision
from .strategy.sma_policy_strategy import SmaWithFilterStrategy

SMA_RUNTIME_BOUNDARY_STAGES = {
    "pre_decision_normalization": "engine.normalize_position_state_before_strategy_decision",
    "snapshot_builder": "runtime_sma_snapshot_builder.build_sma_with_filter_runtime_decision_from_normalized_db",
    "pure_policy": "core.sma_policy.evaluate_sma_policy",
    "final_decision_assembler": "strategy.sma_decision_assembler.evaluate_sma_final_decision",
    "execution_planner": "run_loop_execution_planner.ExecutionPlanner",
    "broker_submit_path": "engine.submit_or_suppress",
}


def _code_provenance() -> dict[str, object]:
    repo_root = Path(__file__).resolve().parents[2]
    base: dict[str, object] = {
        "schema_version": 1,
        "source": "unavailable",
        "commit_sha": None,
        "dirty": None,
        "reason": None,
    }
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            text=True,
            capture_output=True,
            timeout=2.0,
            check=True,
        ).stdout.strip()
        dirty_result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repo_root,
            text=True,
            capture_output=True,
            timeout=2.0,
            check=True,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        base["reason"] = f"git_metadata_unavailable:{type(exc).__name__}"
        return base
    return {
        "schema_version": 1,
        "source": "git",
        "commit_sha": commit or None,
        "dirty": bool(dirty_result.stdout.strip()),
        "reason": None,
    }


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
    """Deprecated replay/debug no-op adapter.

    Replay now enters the normalized read-only snapshot builder directly. This
    adapter remains only for older tests/imports that asserted no-op behavior.
    """

    def normalize_and_persist(self, conn: sqlite3.Connection, **kwargs: object) -> int:
        return 0


def _runtime_snapshot_from_db(
    conn: sqlite3.Connection,
    strategy: SmaWithFilterStrategy,
    *,
    through_ts_ms: int | None = None,
) -> StrategyDecision | None:
    return runtime_sma_snapshot_builder.decide_sma_with_filter_snapshot_from_db(
        conn,
        strategy,
        through_ts_ms=through_ts_ms,
    )


def _runtime_typed_snapshot_from_db(
    conn: sqlite3.Connection,
    strategy: SmaWithFilterStrategy,
    *,
    through_ts_ms: int | None = None,
    boundary_telemetry: dict[str, object] | None = None,
) -> RuntimeSmaDecisionResult | None:
    return runtime_sma_snapshot_builder.decide_sma_with_filter_runtime_snapshot_from_db(
        conn,
        strategy,
        through_ts_ms=through_ts_ms,
        boundary_telemetry=boundary_telemetry,
    )


def decide_sma_with_filter_snapshot_from_db(
    conn: sqlite3.Connection,
    strategy: SmaWithFilterStrategy,
    *,
    through_ts_ms: int | None = None,
) -> StrategyDecision | None:
    """Read-only runtime boundary for SMA DB state -> snapshots -> typed final decision.

    The legacy ``Strategy.decide(conn)`` facade remains available for older
    callers, but live/replay orchestration should bind here after any required
    position normalization has already completed.
    """
    decision = _runtime_snapshot_from_db(
        conn,
        strategy,
        through_ts_ms=through_ts_ms,
    )
    return decision


def decide_sma_with_filter_runtime_snapshot_from_db(
    conn: sqlite3.Connection,
    strategy: SmaWithFilterStrategy,
    *,
    through_ts_ms: int | None = None,
    boundary_telemetry: dict[str, object] | None = None,
) -> RuntimeSmaDecisionResult | None:
    """Typed read-only runtime boundary for SMA DB state -> snapshots -> final decision."""
    return _runtime_typed_snapshot_from_db(
        conn,
        strategy,
        through_ts_ms=through_ts_ms,
        boundary_telemetry=boundary_telemetry,
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
    typed_result = runtime_sma_snapshot_builder.build_sma_with_filter_runtime_decision_from_normalized_db(
        conn,
        strategy,
        through_ts_ms=int(through_ts_ms),
    )
    if typed_result is None:
        return None
    decision = typed_result.legacy_strategy_decision()
    strategy_payload = decision.as_dict()
    context = dict(decision.context)
    execution_summary = build_typed_execution_decision_summary(
        typed_input=TypedExecutionPlanningInput(
            strategy_decision=typed_result.decision,
            candle_ts=typed_result.candle_ts,
            market_price=typed_result.market_price,
            readiness=ExecutionReadinessPlanningInput.from_payload(readiness_payload),
            target=ExecutionTargetPlanningInput(
                previous_target_exposure_krw=previous_target_exposure_krw
            ),
        )
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
        "boundary": context.get("boundary"),
        "normalization_boundary": context.get("normalization_boundary"),
        "normalization_updated_count": context.get("normalization_updated_count"),
        "post_normalization_read_only_guard": context.get("post_normalization_read_only_guard"),
        "post_decision_total_changes_delta": context.get("post_decision_total_changes_delta"),
        "decision_boundary_phase": context.get("decision_boundary_phase"),
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
        "decision_input_bundle_hash": context.get("decision_input_bundle_hash"),
        "decision_input_contract_hash": context.get("decision_input_contract_hash"),
        "decision_input_bundle_payload_hash": context.get("decision_input_bundle_payload_hash"),
        "snapshot_projector_version": context.get("snapshot_projector_version"),
        "snapshot_projector_hash": context.get("snapshot_projector_hash"),
        "materialized_parameters_hash": context.get("materialized_parameters_hash"),
        "market_snapshot_hash": context.get("market_snapshot_hash"),
        "market_feature_hash": context.get("market_feature_hash"),
        "canonical_feature_projection_hash": context.get("canonical_feature_projection_hash"),
        "final_exit_decision_input_hash": context.get("final_exit_decision_input_hash"),
        "position_snapshot_hash": context.get("position_snapshot_hash"),
        "execution_constraints_hash": context.get("execution_constraints_hash"),
        "policy_config_hash": context.get("policy_config_hash"),
        "exit_policy_config_hash": context.get("exit_policy_config_hash"),
        "pure_policy_hash": context.get("pure_policy_hash"),
        "replay_fingerprint": context.get("replay_fingerprint"),
        "pure_policy_trace": pure_policy_trace,
        "final_strategy_decision": strategy_payload,
        "final_typed_strategy_decision": _typed_strategy_decision_payload(typed_result),
        "execution_decision_reconstructable": execution_decision_reconstructable,
        "execution_decision_reconstruction_reason": execution_decision_reconstruction_reason,
        "execution_decision_summary": execution_summary.as_dict(),
    }
