from __future__ import annotations

import json
from dataclasses import dataclass, field
from math import isfinite
from numbers import Real
from typing import Any

from bithumb_bot.market_regime import MARKET_REGIME_VERSION
from bithumb_bot.research.executor import ResearchWorkResult
from bithumb_bot.research.hashing import sha256_prefixed
from bithumb_bot.research.metrics_contract import METRICS_SCHEMA_VERSION, ClosedTradeRecord
from bithumb_bot.research.validation_protocol import EvaluationContext


def minimal_metrics(
    *,
    return_pct: float = 12.5,
    max_drawdown_pct: float = 1.0,
    profit_factor: float | None = 2.0,
    trade_count: int = 4,
) -> dict[str, Any]:
    return {
        "return_pct": return_pct,
        "max_drawdown_pct": max_drawdown_pct,
        "profit_factor": profit_factor,
        "profit_factor_unbounded": profit_factor is None,
        "trade_count": trade_count,
        "win_rate": 0.75,
        "avg_win": 1200.0,
        "avg_loss": -400.0,
        "fee_total": 0.0,
        "slippage_total": 0.0,
        "max_consecutive_losses": 1,
        "single_trade_dependency_score": 0.25,
        "parameter_stability_score": 1.0,
    }


def minimal_metrics_v2(
    *,
    return_pct: float = 12.5,
    cagr_pct: float = 12.5,
    max_drawdown_pct: float = 1.0,
    trade_count: int = 4,
) -> dict[str, Any]:
    return {
        "metrics_schema_version": METRICS_SCHEMA_VERSION,
        "evaluation_status": "completed",
        "metrics_status": "complete",
        "metrics_v2_source": "contract_factory",
        "candidate_failed_before_complete_metrics": False,
        "return_risk": {
            "total_return_pct": return_pct,
            "cagr_pct": cagr_pct,
            "max_drawdown_pct": max_drawdown_pct,
            "realized_return_pct": return_pct,
            "unrealized_pnl_end": 0.0,
            "open_position_at_end": False,
        },
        "trade_quality": {
            "closed_trade_count": trade_count,
            "execution_count": trade_count,
            "win_rate": 0.75,
            "avg_win": 1200.0,
            "avg_loss": -400.0,
            "payoff_ratio": 3.0,
            "profit_factor": 2.0,
            "profit_factor_unbounded": False,
            "expectancy_per_trade_krw": 500.0,
            "expectancy_per_trade_pct": 0.5,
            "max_consecutive_losses": 1,
            "single_trade_dependency_score": 0.25,
        },
        "time_exposure": {
            "period_start_ts": 0,
            "period_end_ts": 60_000,
            "elapsed_ms": 60_000,
            "calendar_days": 1.0,
            "active_bar_count": 2,
            "exposure_time_pct": 5.0,
            "avg_holding_time_ms": 60_000,
            "median_holding_time_ms": 60_000,
            "max_holding_time_ms": 60_000,
        },
        "cost_execution": {
            "fee_total": 0.0,
            "slippage_total": 0.0,
            "fee_drag_ratio": 0.0,
            "slippage_drag_ratio": 0.0,
            "filled_execution_count": trade_count,
            "partial_fill_count": 0,
            "failed_execution_count": 0,
            "skipped_execution_count": 0,
            "quote_coverage_pct": 100.0,
            "median_quote_age_ms": 0.0,
            "p95_quote_age_ms": 0.0,
            "fee_drag_ratio_basis": "traded_notional",
            "slippage_drag_ratio_basis": "traded_notional",
        },
        "limitation_reasons": [],
    }


def minimal_resource_usage(*, label: str = "validation") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "experiment_id": "contract_experiment",
        "behavior_hash": sha256_prefixed({"label": label, "behavior": "factory"}),
        "decision_behavior_hash": sha256_prefixed({"label": label, "decision": "factory"}),
        "trade_ledger_hash": sha256_prefixed({"label": label, "trade": "factory"}),
        "equity_curve_hash": sha256_prefixed({"label": label, "equity": "factory"}),
        "composite_behavior_hash": sha256_prefixed({"label": label, "composite": "factory"}),
        "common_decision_behavior_hash": sha256_prefixed({"label": label, "common": "factory"}),
        "strategy_behavior_hash": sha256_prefixed({"label": label, "strategy": "factory"}),
        "composite_behavior_hash_v2": sha256_prefixed({"label": label, "composite_v2": "factory"}),
    }


def minimal_candidate_base_result(
    *,
    index: int,
    candidate_id: str,
    parameter_values: dict[str, Any],
    include_final_holdout: bool,
    include_walk_forward: bool = False,
) -> dict[str, Any]:
    metrics = minimal_metrics()
    metrics_v2 = minimal_metrics_v2()
    regime_performance = [
        {
            "dimension": "composite_regime",
            "regime": "neutral",
            "trade_count": 1,
            "candle_count": 2,
            "candle_share": 1.0,
            "gross_pnl": 1000.0,
            "net_pnl": 1000.0,
            "return_pct": 1.0,
            "profit_factor": 2.0,
            "profit_factor_unbounded": False,
            "win_rate": 1.0,
            "expectancy": 1000.0,
            "max_drawdown": 0.0,
            "max_consecutive_losses": 0,
            "fee_drag": 0.0,
            "slippage_drag": 0.0,
            "single_trade_dependency_score": 0.25,
        }
    ]
    regime_coverage = [
        {
            "dimension": "composite_regime",
            "regime": "neutral",
            "candle_count": 2,
            "candle_share": 1.0,
            "trade_count": 1,
        }
    ]
    final_metrics = minimal_metrics(return_pct=9.0) if include_final_holdout else None
    final_metrics_v2 = minimal_metrics_v2(return_pct=9.0, cagr_pct=9.0) if include_final_holdout else None
    closed_trades = (
        ClosedTradeRecord(exit_ts=60_000, net_pnl=1000.0, return_pct=0.1, entry_ts=0),
        ClosedTradeRecord(exit_ts=120_000, net_pnl=-10_000.0, return_pct=-1.0, entry_ts=60_000),
        ClosedTradeRecord(exit_ts=180_000, net_pnl=20_000.0, return_pct=2.0, entry_ts=120_000),
    )
    execution_metadata = [
        {
            "side": "BUY",
            "requested_qty": 1.0,
            "filled_qty": 1.0,
            "fill_status": "filled",
            "execution_model": "contract_factory",
        }
    ]
    walk_forward = (
        {
            "return_consistency_pass": True,
            "window_count": 1,
            "windows": [
                {
                    "window_id": "window_001",
                    "train_metrics": metrics,
                    "test_metrics": metrics,
                    "return_consistency_pass": True,
                }
            ],
        }
        if include_walk_forward
        else None
    )
    return {
        "index": index,
        "candidate_id": candidate_id,
        "candidate_failed": False,
        "candidate_failed_before_complete_metrics": False,
        "evaluation_status": "completed",
        "metrics_status": "complete",
        "metrics_v2_source": "contract_factory",
        "parameter_values": dict(parameter_values),
        "train_metrics": metrics,
        "validation_metrics": metrics,
        "final_holdout_metrics": final_metrics,
        "train_metrics_v2": metrics_v2,
        "validation_metrics_v2": metrics_v2,
        "final_holdout_metrics_v2": final_metrics_v2,
        "train_closed_trades": closed_trades,
        "validation_closed_trades": closed_trades,
        "final_holdout_closed_trades": closed_trades if include_final_holdout else (),
        "train_equity_curve": [],
        "validation_equity_curve": [],
        "final_holdout_equity_curve": [],
        "train_execution_metadata": execution_metadata,
        "validation_execution_metadata": execution_metadata,
        "final_holdout_execution_metadata": execution_metadata if include_final_holdout else None,
        "train_execution_event_summary": {},
        "validation_execution_event_summary": {},
        "final_holdout_execution_event_summary": {} if include_final_holdout else None,
        "train_strategy_diagnostics": {"source": "contract_factory"},
        "validation_strategy_diagnostics": {"source": "contract_factory"},
        "final_holdout_strategy_diagnostics": {"source": "contract_factory"} if include_final_holdout else None,
        "train_regime_performance": regime_performance,
        "train_regime_coverage": regime_coverage,
        "validation_regime_performance": regime_performance,
        "validation_regime_coverage": regime_coverage,
        "final_holdout_regime_performance": regime_performance if include_final_holdout else None,
        "final_holdout_regime_coverage": regime_coverage if include_final_holdout else None,
        "walk_forward_metrics": walk_forward,
        "warnings": [],
        "train_resource_usage": minimal_resource_usage(label="train"),
        "validation_resource_usage": minimal_resource_usage(label="validation"),
        "final_holdout_resource_usage": (
            minimal_resource_usage(label="final_holdout") if include_final_holdout else None
        ),
        "train_audit_trace_index": None,
        "validation_audit_trace_index": None,
        "final_holdout_audit_trace_index": None,
        "retained_detail_summary": {"report_detail": "summary", "source": "contract_factory"},
    }


@dataclass
class DeterministicResearchEvaluator:
    completed_calls: list[dict[str, Any]] = field(default_factory=list)

    def evaluate(self, work_unit, context: EvaluationContext) -> ResearchWorkResult:
        candidate_id = work_unit.candidate_id
        candles_processed = sum(len(snapshot.candles) for snapshot in context.snapshots.values())
        base = minimal_candidate_base_result(
            index=context.candidate_index,
            candidate_id=candidate_id,
            parameter_values=context.params,
            include_final_holdout="final_holdout" in context.snapshots,
            include_walk_forward=context.include_walk_forward,
        )
        scenario_payload = context.scenario.as_dict() if hasattr(context.scenario, "as_dict") else {}
        if scenario_payload.get("type") == "stress":
            seed = int(scenario_payload.get("seed") or 0)
            seed_inputs = {
                "base_seed": seed,
                "scenario_id": context.scenario_id,
                "parameter_candidate_id": candidate_id,
                "split_name": "validation",
            }
            stress_metadata = {
                "base_seed": seed,
                "derived_seed_hash": sha256_prefixed(seed_inputs),
                "seed_derivation_inputs": seed_inputs,
            }
            for key in ("train_execution_metadata", "validation_execution_metadata", "final_holdout_execution_metadata"):
                rows = base.get(key)
                if isinstance(rows, list):
                    for row in rows:
                        if isinstance(row, dict):
                            row.update(stress_metadata)
        observability = {
            "work_unit": work_unit.as_dict(),
            "status": "completed",
            "wall_seconds": 0.0,
            "cpu_seconds": 0.0,
            "candles_processed": candles_processed,
            "candles_per_second": None,
            "split_results": [
                {
                    "split_name": split_name,
                    "status": "completed",
                    "wall_seconds": 0.0,
                    "cpu_seconds": 0.0,
                    "candles_processed": len(snapshot.candles),
                    "evaluator": "deterministic_contract",
                }
                for split_name, snapshot in sorted(context.snapshots.items())
            ],
        }
        observability["content_hash"] = sha256_prefixed(
            {
                "work_unit_hash": work_unit.work_unit_hash,
                "status": "completed",
                "evaluator": "deterministic_contract",
                "candles_processed": candles_processed,
            }
        )
        self.completed_calls.append({"work_unit": work_unit.as_dict(), "context": context})
        return ResearchWorkResult(
            work_unit=work_unit,
            work_unit_hash=work_unit.work_unit_hash,
            candidate_index=context.candidate_index,
            candidate_id=candidate_id,
            scenario_index=context.scenario_index,
            scenario_id=context.scenario_id,
            status="completed",
            base_result=base,
            observability=observability,
        )


def minimal_scenario_result(**overrides: Any) -> dict[str, Any]:
    payload = {
        "scenario_id": "scenario_contract",
        "scenario_index": 0,
        "scenario_type": "fixed_bps",
        "scenario_role": "base",
        "train_metrics": minimal_metrics(),
        "validation_metrics": minimal_metrics(),
        "metrics_gate_policy": {"schema_version": 1},
        "metrics_gate_policy_hash": sha256_prefixed({"schema_version": 1}),
        "behavior_hash": minimal_resource_usage()["behavior_hash"],
        "strategy_behavior_hash": minimal_resource_usage()["strategy_behavior_hash"],
        "scenario_acceptance_gate_result": "PASS",
        "scenario_fail_reasons": [],
        "regime_classifier_version": MARKET_REGIME_VERSION,
    }
    payload.update(overrides)
    return payload


def minimal_candidate_payload(**overrides: Any) -> dict[str, Any]:
    scenario = minimal_scenario_result()
    payload = {
        "experiment_id": "contract_experiment",
        "manifest_hash": sha256_prefixed({"manifest": "contract"}),
        "dataset_snapshot_id": "contract_snapshot",
        "dataset_content_hash": sha256_prefixed({"dataset": "contract"}),
        "dataset_quality_hash": sha256_prefixed({"quality": "contract"}),
        "strategy_name": "sma_with_filter",
        "parameter_candidate_id": "candidate_contract",
        "parameter_values": {"SMA_SHORT": 2, "SMA_LONG": 4},
        "parameter_values_raw": {"SMA_SHORT": 2, "SMA_LONG": 4},
        "scenario_results": [scenario],
        "acceptance_gate_result": "PASS",
        "gate_fail_reasons": [],
        "metrics_gate_policy": scenario["metrics_gate_policy"],
        "metrics_gate_policy_hash": scenario["metrics_gate_policy_hash"],
        "behavior_hash": scenario["behavior_hash"],
        "strategy_behavior_hash": scenario["strategy_behavior_hash"],
        "candidate_behavior_profile_hash": sha256_prefixed({"candidate": "behavior"}),
        "candidate_profile_hash": sha256_prefixed({"candidate": "profile"}),
    }
    payload.update(overrides)
    return payload


def minimal_research_report(**overrides: Any) -> dict[str, Any]:
    candidate = minimal_candidate_payload()
    payload = {
        "report_kind": "backtest",
        "experiment_id": "contract_experiment",
        "manifest_hash": sha256_prefixed({"manifest": "contract"}),
        "candidate_count": 1,
        "candidates": [candidate],
        "workload_estimate": {
            "candidate_count": 1,
            "scenario_count": 1,
            "split_count": 2,
            "walk_forward_window_count": 0,
            "estimated_strategy_runs": 2,
            "estimated_tick_events": 2,
            "approx_snapshot_candle_count": 2,
            "audit_mode": "summary_only",
            "report_detail": "summary",
            "full_decisions_external_jsonl": False,
            "estimated_audit_stream_rows": 0,
            "estimated_artifact_write_count": 2,
            "estimated_hash_payload_bytes": 4096,
            "estimated_snapshot_hash_count": 1,
            "uses_production_evaluator": False,
            "uses_real_parallel_executor": False,
        },
        "audit_trail_policy": {"mode": "summary_only"},
        "audit_trail_status": "DISABLED",
        "execution_observability": {
            "production_evaluator_used": False,
            "contract_evaluator_used": True,
            "parallel_executor_used": False,
        },
        "content_hash": sha256_prefixed({"report": "contract"}),
    }
    payload.update(overrides)
    return payload


def assert_fast_research_workload(
    report: dict[str, Any],
    *,
    max_strategy_runs: int = 3,
    max_tick_events: int = 10_000,
    max_matrix_size: int = 3,
    max_walk_forward_windows: int = 0,
    allow_complete_external_audit: bool = False,
    allow_full_report_detail: bool = False,
    allow_full_decisions_external_jsonl: bool = False,
    max_audit_stream_rows: int = 0,
    max_artifact_write_count: int = 4,
    max_hash_payload_bytes: int = 1_000_000,
    max_snapshot_hash_count: int = 3,
    allow_production_evaluator: bool = False,
    allow_real_parallel_executor: bool = False,
) -> None:
    estimate = report.get("workload_estimate")
    if not isinstance(estimate, dict):
        execution_plan = report.get("execution_plan")
        estimate = execution_plan.get("workload_estimate") if isinstance(execution_plan, dict) else None
    assert isinstance(estimate, dict), "research report must expose workload_estimate"
    for key in (
        "candidate_count",
        "scenario_count",
        "split_count",
        "walk_forward_window_count",
        "estimated_strategy_runs",
        "estimated_tick_events",
        "approx_snapshot_candle_count",
        "audit_mode",
        "report_detail",
        "full_decisions_external_jsonl",
        "estimated_audit_stream_rows",
        "estimated_artifact_write_count",
        "estimated_hash_payload_bytes",
        "estimated_snapshot_hash_count",
        "uses_production_evaluator",
        "uses_real_parallel_executor",
    ):
        assert key in estimate, f"research workload_estimate missing {key}"
    def required_int(key: str) -> int:
        value = estimate[key]
        assert isinstance(value, Real) and not isinstance(value, bool), (
            f"research workload_estimate {key} must be a known numeric value"
        )
        assert isfinite(float(value)), f"research workload_estimate {key} must be finite"
        assert int(value) == value, f"research workload_estimate {key} must be an integer value"
        assert int(value) >= 0, f"research workload_estimate {key} must be non-negative"
        return int(value)

    assert required_int("estimated_strategy_runs") <= max_strategy_runs
    assert required_int("estimated_tick_events") <= max_tick_events
    if not allow_complete_external_audit:
        assert estimate.get("audit_mode") != "complete_external"
    assert required_int("walk_forward_window_count") <= max_walk_forward_windows
    if not allow_full_report_detail:
        assert estimate.get("report_detail") == "summary"
    if not allow_full_decisions_external_jsonl:
        assert estimate.get("full_decisions_external_jsonl") is not True
    assert required_int("estimated_audit_stream_rows") <= max_audit_stream_rows
    assert required_int("estimated_artifact_write_count") <= max_artifact_write_count
    estimated_hash_payload_bytes = required_int("estimated_hash_payload_bytes")
    assert estimated_hash_payload_bytes <= max_hash_payload_bytes, (
        "research workload_estimate estimated_hash_payload_bytes exceeded budget: "
        f"actual={estimated_hash_payload_bytes} max={max_hash_payload_bytes} "
        f"estimate={json.dumps(estimate, sort_keys=True, default=repr)}"
    )
    assert required_int("estimated_snapshot_hash_count") <= max_snapshot_hash_count
    uses_production_evaluator = estimate["uses_production_evaluator"]
    uses_real_parallel_executor = estimate["uses_real_parallel_executor"]
    assert isinstance(uses_production_evaluator, bool), (
        "research workload_estimate uses_production_evaluator must be an explicit bool"
    )
    assert isinstance(uses_real_parallel_executor, bool), (
        "research workload_estimate uses_real_parallel_executor must be an explicit bool"
    )
    if not allow_production_evaluator:
        assert uses_production_evaluator is False
    if not allow_real_parallel_executor:
        assert uses_real_parallel_executor is False
    matrix_size = (
        required_int("candidate_count")
        * required_int("scenario_count")
        * required_int("split_count")
    )
    assert matrix_size <= max_matrix_size
