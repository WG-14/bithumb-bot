from __future__ import annotations

import json
from pathlib import Path

from bithumb_bot.research.report_writer import (
    ResearchReportPaths,
    build_report_artifacts,
    compute_artifact_write_summary,
    compute_report_hashes,
    persist_final_research_report_observability,
    sync_final_report_observability,
    summarize_candidate_result,
    summarize_derived_candidate,
    summarize_report_candidate,
    write_report_artifacts,
    write_research_report,
)
from tests.test_research_backtest_reproducibility import _research_manager, _summary_report_payload


def _paths(tmp_path: Path) -> ResearchReportPaths:
    return ResearchReportPaths(
        derived_path=tmp_path / "derived_candidates.json",
        report_path=tmp_path / "backtest_report.json",
        candidate_events_path=tmp_path / "candidate_events.jsonl",
        candidate_results_dir=tmp_path / "candidate_results",
        candidate_failures_dir=tmp_path / "candidate_failures",
        trace_manifest_path=tmp_path / "trace_manifest.json",
    )


def _artifact_summary() -> dict[str, object]:
    return {
        "schema_version": 1,
        "derived_candidates_path": "/tmp/derived_candidates.json",
        "derived_candidates_ref": "derived/research/test/derived_candidates.json",
        "derived_candidates_hash": "sha256:" + "0" * 64,
        "derived_candidates_bytes": 17,
        "report_path": "/tmp/backtest_report.json",
        "report_ref": "reports/research/test/backtest_report.json",
        "report_bytes": 0,
        "artifact_file_count": 2,
        "artifact_total_bytes": 17,
        "write_wall_seconds": 0.25,
    }


def test_report_write_stage_timing_payload_matches_artifact_summary(tmp_path: Path) -> None:
    payload = {
        "experiment_id": "contract",
        "candidates": [],
        "execution_observability": {
            "stage_timings": [
                {"stage": "load_split", "wall_seconds": 0.1},
                {"stage": "report_write", "wall_seconds": 0.2},
            ]
        },
    }

    _, summary = persist_final_research_report_observability(
        paths=_paths(tmp_path),
        report_payload=payload,
        artifact_write_summary=_artifact_summary(),
        artifact_total_bytes_base=17,
    )

    report_write = [
        item for item in payload["execution_observability"]["stage_timings"] if item["stage"] == "report_write"
    ][0]
    assert report_write["artifact_total_bytes"] == summary["artifact_total_bytes"]
    assert report_write["artifact_file_count"] == summary["artifact_file_count"]
    assert report_write["derived_candidates_bytes"] == summary["derived_candidates_bytes"]
    assert report_write["report_bytes"] == summary["report_bytes"]


def test_summary_report_references_candidate_detail_artifacts(tmp_path: Path, monkeypatch) -> None:
    manager = _research_manager(tmp_path, monkeypatch)
    result = write_research_report(
        manager=manager,
        experiment_id="summary_candidate_refs",
        report_name="backtest",
        payload=_summary_report_payload(experiment_id="summary_candidate_refs"),
    )
    persisted = json.loads(result.paths.report_path.read_text(encoding="utf-8"))

    assert persisted["artifact_refs"]["candidate_results_dir"] == (
        "derived/research/summary_candidate_refs/candidate_results"
    )
    assert persisted["artifact_refs"]["audit_trace_manifest"] == (
        "derived/research/summary_candidate_refs/trace_manifest.json"
    )
    assert persisted["artifact_paths"]["candidate_results_dir"] == str(result.paths.candidate_results_dir.resolve())
    assert persisted["artifact_paths"]["audit_trace_manifest_path"] == str(result.paths.trace_manifest_path.resolve())


def test_report_summary_preserves_parallel_efficiency_summary(tmp_path: Path, monkeypatch) -> None:
    manager = _research_manager(tmp_path, monkeypatch)
    payload = _summary_report_payload(experiment_id="parallel_efficiency_summary")
    payload.setdefault("research_run", {})["report_detail"] = "summary"
    payload.setdefault("execution_observability", {})["parallel_efficiency"] = {
        "requested_max_workers": 8,
        "effective_max_workers": 8,
        "available_parallel_work_tasks": 1,
        "observed_worker_count": 1,
        "expected_worker_utilization_pct": 12.5,
        "observed_worker_utilization_pct": 12.5,
        "worker_warning_reasons": [],
        "worker_observation_warning_reasons": ["observed_workers_below_effective"],
    }

    result = write_research_report(
        manager=manager,
        experiment_id="parallel_efficiency_summary",
        report_name="backtest",
        payload=payload,
    )
    persisted = json.loads(result.paths.report_path.read_text(encoding="utf-8"))

    efficiency = persisted["execution_observability"]["parallel_efficiency"]
    assert efficiency["requested_max_workers"] == 8
    assert efficiency["available_parallel_work_tasks"] == 1
    assert efficiency["worker_observation_warning_reasons"] == ["observed_workers_below_effective"]


def test_persist_final_research_report_observability_updates_persisted_payload(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    payload = {
        "experiment_id": "contract",
        "candidates": [],
        "execution_observability": {
            "stage_timings": [
                {"stage": "report_write", "wall_seconds": 0.2},
            ]
        },
    }

    content_hash, summary = persist_final_research_report_observability(
        paths=paths,
        report_payload=payload,
        artifact_write_summary=_artifact_summary(),
        artifact_total_bytes_base=17,
    )

    persisted = json.loads(paths.report_path.read_text(encoding="utf-8"))
    assert persisted["content_hash"] == content_hash
    assert persisted["artifact_write_summary"] == summary
    assert persisted["artifact_observability"]["report_write"] == summary


def test_report_write_observability_records_substage_timings(tmp_path: Path, monkeypatch) -> None:
    manager = _research_manager(tmp_path, monkeypatch)

    result = write_research_report(
        manager=manager,
        experiment_id="report_write_substages",
        report_name="backtest",
        payload=_summary_report_payload(experiment_id="report_write_substages"),
    )

    persisted = json.loads(result.paths.report_path.read_text(encoding="utf-8"))
    report_write = persisted["artifact_observability"]["report_write"]
    substages = {item["stage"]: item for item in report_write["substage_timings"]}
    for stage in {
        "reference_first_payload",
        "report_candidate_summary",
        "derived_candidate_summary",
        "report_hashing",
        "report_byte_count",
        "write_derived",
        "write_report",
        "persist_final_observability",
        "final_report_rewrite",
    }:
        assert stage in substages
        assert substages[stage]["wall_seconds"] >= 0
    assert report_write["file_write_wall_seconds"] >= 0
    stage_names = {item["stage"] for item in persisted["execution_observability"]["stage_timings"]}
    assert "report_write.write_derived" in stage_names
    assert "report_write.write_report" in stage_names
    assert "report_write.persist_final_observability" in stage_names


def test_report_writer_exposes_build_hash_write_sync_steps() -> None:
    assert callable(build_report_artifacts)
    assert callable(compute_report_hashes)
    assert callable(compute_artifact_write_summary)
    assert callable(write_report_artifacts)
    assert callable(sync_final_report_observability)


def test_final_observability_sync_does_not_require_validation_protocol_rewrite(tmp_path: Path, monkeypatch) -> None:
    manager = _research_manager(tmp_path, monkeypatch)

    result = write_research_report(
        manager=manager,
        experiment_id="writer_final_sync",
        report_name="backtest",
        payload=_summary_report_payload(experiment_id="writer_final_sync"),
    )

    persisted = json.loads(result.paths.report_path.read_text(encoding="utf-8"))
    assert persisted["artifact_write_summary"] == result.artifact_write_summary
    assert persisted["content_hash"] == result.content_hash
    assert persisted["artifact_write_summary"]["report_bytes"] == result.paths.report_path.stat().st_size


def test_summary_report_uses_candidate_summary() -> None:
    candidate = {
        "candidate_id": "candidate_001",
        "acceptance_gate_result": "PASS",
        "validation_metrics_v2": {"total_return_pct": 1.0},
        "decisions": [{"ts": 1}],
        "equity_curve": [{"ts": 1, "equity": 1.0}],
    }

    summary = summarize_report_candidate(candidate)

    assert summary["candidate_id"] == "candidate_001"
    assert summary["acceptance_gate_result"] == "PASS"
    assert summary["validation_metrics_v2"] == {"total_return_pct": 1.0}
    assert summary["candidate_payload_hash"].startswith("sha256:")
    assert "decisions" not in summary
    assert "equity_curve" not in summary


def test_report_summary_keeps_participation_metric_hash() -> None:
    candidate = {
        "candidate_id": "candidate_001",
        "validation_metrics_v2": {
            "metrics_schema_version": 2,
            "participation": {
                "timezone": "Asia/Seoul",
                "count_basis": "filled",
                "calendar_day_count": 3,
                "days_with_intent": 3,
                "days_with_submit_expected": 3,
                "days_with_filled_execution": 1,
                "days_with_closed_trade": 1,
                "zero_filled_days": 2,
                "max_consecutive_zero_filled_days": 2,
                "daily_counts_hash": "sha256:" + "5" * 64,
            },
        },
    }

    summary = summarize_report_candidate(candidate)

    assert summary["participation_metric_hash"].startswith("sha256:")
    assert summary["participation_summary"]["zero_filled_days"] == 2


def test_summary_derived_candidates_are_bounded() -> None:
    candidate = {
        "parameter_candidate_id": "candidate_001",
        "candidate_profile_hash": "sha256:profile",
        "scenario_results": [
            {
                "scenario_id": "scenario_001",
                "validation_equity_curve": [{"ts": 1, "equity": 1.0}],
                "train_equity_curve": [{"ts": 1, "equity": 1.0}],
                "final_holdout_equity_curve": [{"ts": 1, "equity": 1.0}],
                "equity_curve_hash": "sha256:equity",
                "retained_detail_summary": {"retained_equity_point_count": 0},
            }
        ],
        "decisions": [{"ts": 1}],
    }

    summary = summarize_derived_candidate(candidate, "summary")

    assert summary["derived_detail_policy"] == "summary_bounded"
    assert summary["candidate_result_detail_policy"] == "summary_bounded"
    assert summary["candidate_profile_hash"] == "sha256:profile"
    assert "decisions" not in summary
    scenario = summary["scenario_results"][0]
    assert scenario["train_equity_curve"] == []
    assert scenario["validation_equity_curve"] == []
    assert scenario["final_holdout_equity_curve"] == []
    assert scenario["equity_curve_hash"] == "sha256:equity"
    assert scenario["retained_detail_summary"] == {"retained_equity_point_count": 0}


def test_candidate_result_summary_is_reference_first_bounded() -> None:
    candidate = {
        "parameter_candidate_id": "candidate_001",
        "candidate_profile_hash": "sha256:profile",
        "behavior_hash": "sha256:behavior",
        "scenario_results": [
            {
                "scenario_id": "scenario_001",
                "validation_equity_curve": [{"ts": 1, "equity": 1.0}],
                "validation_execution_metadata": [{"ts": 1, "fill": "large"}],
                "behavior_hash": "sha256:scenario-behavior",
                "equity_curve_hash": "sha256:equity",
                "retained_detail_summary": {"retained_equity_point_count": 0},
            }
        ],
    }

    summary = summarize_candidate_result(candidate, "summary")

    assert summary["candidate_result_detail_policy"] == "summary_bounded"
    assert summary["candidate_profile_hash"] == "sha256:profile"
    assert summary["behavior_hash"] == "sha256:behavior"
    scenario = summary["scenario_results"][0]
    assert scenario["validation_equity_curve"] == []
    assert "validation_execution_metadata" not in scenario
    assert scenario["behavior_hash"] == "sha256:scenario-behavior"
    assert scenario["equity_curve_hash"] == "sha256:equity"


def _cost_scenario(role: str, fee_rate: float, fee_source: str) -> dict[str, object]:
    return {
        "scenario_id": f"scenario_{role}",
        "scenario_index": 0 if role == "base" else 1,
        "scenario_role": role,
        "scenario_acceptance_gate_result": "PASS" if role == "base" else "FAIL",
        "cost_model": {"fee_rate": fee_rate, "slippage_bps": 10.0 if role == "base" else 20.0},
        "cost_assumption": {
            "label": f"{role}_fee",
            "role": role,
            "fee_rate": fee_rate,
            "fee_source": fee_source,
            "fee_authority_policy": "runtime_fee_authority_must_match_or_fail"
            if role == "base"
            else "not_promotable_as_runtime_base",
            "slippage_bps": 10.0 if role == "base" else 20.0,
            "slippage_source": fee_source,
            "promotable_as_base": role == "base",
            "source": "execution_model",
        },
        "validation_metrics": {"return_pct": 1.0 if role == "base" else -1.0, "trade_count": 3},
        "final_holdout_metrics": {"return_pct": 0.5},
        "execution_model_hash": f"sha256:{role}",
        "model_params_hash": f"sha256:{role}",
    }


def test_summary_scenario_results_preserve_cost_authority() -> None:
    candidate = {
        "parameter_candidate_id": "candidate_001",
        "scenario_results": [
            _cost_scenario("base", 0.0004, "operator_declared_bithumb_app_fee"),
            _cost_scenario("stress", 0.0025, "stress_assumption"),
        ],
    }

    summary = summarize_candidate_result(candidate, "summary")

    base, stress = summary["scenario_results"]
    assert base["cost_model"]["fee_rate"] == 0.0004
    assert stress["cost_model"]["fee_rate"] == 0.0025
    assert base["cost_assumption"]["fee_source"] == "operator_declared_bithumb_app_fee"
    assert stress["cost_assumption"]["fee_source"] == "stress_assumption"
    assert stress["cost_assumption"]["promotable_as_base"] is False
    assert base["cost_assumption"]["fee_authority_policy"] == "runtime_fee_authority_must_match_or_fail"


def test_derived_candidate_summary_preserves_base_and_stress_fee_rates() -> None:
    candidate = {
        "parameter_candidate_id": "candidate_001",
        "scenario_results": [
            _cost_scenario("stress", 0.0025, "stress_assumption"),
            _cost_scenario("base", 0.0004, "operator_declared_bithumb_app_fee"),
        ],
    }

    summary = summarize_derived_candidate(candidate, "summary")
    by_role = {scenario["scenario_role"]: scenario for scenario in summary["scenario_results"]}

    assert by_role["base"]["cost_model"]["fee_rate"] == 0.0004
    assert by_role["stress"]["cost_model"]["fee_rate"] == 0.0025
    assert by_role["base"]["cost_assumption"]["fee_source"] == "operator_declared_bithumb_app_fee"
    assert by_role["stress"]["cost_assumption"]["promotable_as_base"] is False


def test_summarize_candidate_result_keeps_compact_diagnostics_fields() -> None:
    candidate = {
        "parameter_candidate_id": "candidate_001",
        "candidate_profile_hash": "sha256:profile",
        "validation_strategy_diagnostics": {
            "raw_signal_count": 3,
            "final_signal_count": 1,
            "entry_count": 1,
            "exit_count": 0,
            "blocked_filter_distribution": {"volume_ratio_below_min": 2},
            "entry_reason_distribution": {"entry_allowed": 1},
            "exit_reason_distribution": {},
            "exit_rule_distribution": {"take_profit": 1},
            "return_by_exit_reason": {
                "take_profit": {
                    "count": 1,
                    "avg_return_pct": 0.02,
                    "total_return_pct": 0.02,
                    "avg_pnl": 10.0,
                    "total_pnl": 10.0,
                }
            },
            "avg_holding_minutes_by_exit_reason": {"take_profit": 5.0},
            "mae_mfe_by_exit_reason": {
                "take_profit": {
                    "count": 1,
                    "avg_mae_pct": -0.003,
                    "min_mae_pct": -0.003,
                    "avg_mfe_pct": 0.025,
                    "max_mfe_pct": 0.025,
                }
            },
            "p95_mae_pct": -1.0,
            "p95_mfe_pct": 2.0,
            "worst_trade_mae_pct": -3.0,
            "strategy_diagnostics_namespace": "channel_breakout_with_regime_filter",
            "mae_pct_by_trade": [-3.0, -1.0],
        },
        "strategy_diagnostics": {"strategy_diagnostics_namespace": "channel_breakout_with_regime_filter"},
        "scenario_results": [
            {
                "scenario_id": "scenario_001",
                "validation_strategy_diagnostics": {
                    "raw_signal_count": 3,
                    "final_signal_count": 1,
                    "entry_count": 1,
                    "exit_count": 0,
                    "blocked_filter_distribution": {"volume_ratio_below_min": 2},
                    "entry_reason_distribution": {"entry_allowed": 1},
                    "exit_reason_distribution": {},
                    "exit_rule_distribution": {"take_profit": 1},
                    "return_by_exit_reason": {
                        "take_profit": {
                            "count": 1,
                            "avg_return_pct": 0.02,
                            "total_return_pct": 0.02,
                            "avg_pnl": 10.0,
                            "total_pnl": 10.0,
                        }
                    },
                    "avg_holding_minutes_by_exit_reason": {"take_profit": 5.0},
                    "mae_mfe_by_exit_reason": {
                        "take_profit": {
                            "count": 1,
                            "avg_mae_pct": -0.003,
                            "min_mae_pct": -0.003,
                            "avg_mfe_pct": 0.025,
                            "max_mfe_pct": 0.025,
                        }
                    },
                    "p95_mae_pct": -1.0,
                    "p95_mfe_pct": 2.0,
                    "worst_trade_mae_pct": -3.0,
                    "strategy_diagnostics_namespace": "channel_breakout_with_regime_filter",
                    "mae_pct_by_trade": [-3.0, -1.0],
                },
            }
        ],
    }

    summary = summarize_candidate_result(candidate, "summary")

    diagnostics = summary["validation_strategy_diagnostics"]
    assert diagnostics["raw_signal_count"] == 3
    assert diagnostics["blocked_filter_distribution"] == {"volume_ratio_below_min": 2}
    assert diagnostics["exit_rule_distribution"] == {"take_profit": 1}
    assert diagnostics["return_by_exit_reason"]["take_profit"]["total_pnl"] == 10.0
    assert diagnostics["avg_holding_minutes_by_exit_reason"] == {"take_profit": 5.0}
    assert diagnostics["mae_mfe_by_exit_reason"]["take_profit"]["max_mfe_pct"] == 0.025
    assert diagnostics["p95_mfe_pct"] == 2.0
    assert "mae_pct_by_trade" not in diagnostics
    assert summary["scenario_results"][0]["validation_strategy_diagnostics"] == diagnostics


def _candidate_with_exit_diagnostics(exit_diagnostics: dict[str, object]) -> dict[str, object]:
    return {
        "parameter_candidate_id": "candidate_001",
        "candidate_profile_hash": "sha256:profile",
        "validation_strategy_diagnostics": {
            "strategy_diagnostics_namespace": "channel_breakout_with_regime_filter",
            **exit_diagnostics,
        },
        "decisions": [{"ts": 1, "signal": "SELL"}],
        "equity_curve": [{"ts": 1, "equity": 1.0}],
        "scenario_results": [
            {
                "scenario_id": "scenario_001",
                "validation_strategy_diagnostics": {
                    "strategy_diagnostics_namespace": "channel_breakout_with_regime_filter",
                    **exit_diagnostics,
                },
                "validation_resource_usage": {"stage_trace": [{"stage": "validation"}]},
            }
        ],
    }


def test_summary_candidate_preserves_exit_rule_distribution() -> None:
    summary = summarize_candidate_result(
        _candidate_with_exit_diagnostics({"exit_rule_distribution": {"take_profit": 1}}),
        "summary",
    )

    assert summary["validation_strategy_diagnostics"]["exit_rule_distribution"] == {"take_profit": 1}
    assert summary["scenario_results"][0]["validation_strategy_diagnostics"]["exit_rule_distribution"] == {
        "take_profit": 1
    }
    assert "decisions" not in summary
    assert "equity_curve" not in summary
    assert "stage_trace" not in summary["scenario_results"][0].get("validation_resource_usage", {})


def test_summary_candidate_preserves_return_by_exit_reason() -> None:
    diagnostics = {
        "return_by_exit_reason": {
            "take_profit": {
                "count": 1,
                "avg_return_pct": 0.02,
                "total_return_pct": 0.02,
                "avg_pnl": 10.0,
                "total_pnl": 10.0,
            }
        }
    }

    summary = summarize_candidate_result(_candidate_with_exit_diagnostics(diagnostics), "summary")

    assert summary["validation_strategy_diagnostics"]["return_by_exit_reason"] == diagnostics["return_by_exit_reason"]
    assert (
        summary["scenario_results"][0]["validation_strategy_diagnostics"]["return_by_exit_reason"]
        == diagnostics["return_by_exit_reason"]
    )
    assert "decisions" not in summary
    assert "equity_curve" not in summary
    assert "stage_trace" not in summary["scenario_results"][0].get("validation_resource_usage", {})


def test_summary_candidate_preserves_avg_holding_minutes_by_exit_reason() -> None:
    diagnostics = {"avg_holding_minutes_by_exit_reason": {"take_profit": 5.0}}

    summary = summarize_candidate_result(_candidate_with_exit_diagnostics(diagnostics), "summary")

    assert (
        summary["validation_strategy_diagnostics"]["avg_holding_minutes_by_exit_reason"]
        == diagnostics["avg_holding_minutes_by_exit_reason"]
    )
    assert (
        summary["scenario_results"][0]["validation_strategy_diagnostics"]["avg_holding_minutes_by_exit_reason"]
        == diagnostics["avg_holding_minutes_by_exit_reason"]
    )
    assert "decisions" not in summary
    assert "equity_curve" not in summary
    assert "stage_trace" not in summary["scenario_results"][0].get("validation_resource_usage", {})


def test_summary_candidate_preserves_mae_mfe_by_exit_reason() -> None:
    diagnostics = {
        "mae_mfe_by_exit_reason": {
            "take_profit": {
                "count": 1,
                "avg_mae_pct": -0.003,
                "min_mae_pct": -0.003,
                "avg_mfe_pct": 0.025,
                "max_mfe_pct": 0.025,
            }
        }
    }

    summary = summarize_candidate_result(_candidate_with_exit_diagnostics(diagnostics), "summary")

    assert summary["validation_strategy_diagnostics"]["mae_mfe_by_exit_reason"] == diagnostics[
        "mae_mfe_by_exit_reason"
    ]
    assert summary["scenario_results"][0]["validation_strategy_diagnostics"]["mae_mfe_by_exit_reason"] == diagnostics[
        "mae_mfe_by_exit_reason"
    ]
    assert "decisions" not in summary
    assert "equity_curve" not in summary
    assert "stage_trace" not in summary["scenario_results"][0].get("validation_resource_usage", {})


def test_summary_candidate_result_keeps_cost_sensitivity_summary() -> None:
    candidate = {
        "parameter_candidate_id": "candidate_001",
        "cost_sensitivity": {
            "zero_cost": {"validation_return_pct": 5.0},
            "base_cost": {"validation_return_pct": 3.0},
            "stress_cost": {"validation_return_pct": 1.0},
            "fee_drag_ratio": 0.4,
            "slippage_drag_ratio": 0.2,
            "cost_breakeven_trade_edge": 10.0,
        },
        "scenario_results": [],
    }

    summary = summarize_candidate_result(candidate, "summary")

    assert summary["cost_sensitivity"]["zero_cost"]["validation_return_pct"] == 5.0
    assert summary["cost_sensitivity"]["fee_drag_ratio"] == 0.4


def test_summary_candidate_result_keeps_position_sizing_sensitivity_summary() -> None:
    candidate = {
        "parameter_candidate_id": "candidate_001",
        "position_sizing_sensitivity": {
            "status": "available",
            "by_buy_fraction": {
                "0.99": {
                    "validation_return_pct": 3.0,
                    "validation_max_drawdown_pct": 1.0,
                    "validation_profit_factor": 2.0,
                    "portfolio_policy_hash": "sha256:policy099",
                },
                "0.50": {
                    "validation_return_pct": 2.0,
                    "validation_max_drawdown_pct": 0.8,
                    "validation_profit_factor": 1.8,
                    "portfolio_policy_hash": "sha256:policy050",
                },
            },
        },
        "scenario_results": [],
    }

    summary = summarize_candidate_result(candidate, "summary")

    assert summary["position_sizing_sensitivity"]["status"] == "available"
    assert summary["position_sizing_sensitivity"]["by_buy_fraction"]["0.50"][
        "validation_profit_factor"
    ] == 1.8


def test_summary_candidate_result_keeps_runtime_capability_summary() -> None:
    candidate = {
        "parameter_candidate_id": "candidate_001",
        "strategy_runtime_capabilities": {
            "research_only": True,
            "promotion_runtime_decisions_supported": False,
            "runtime_replay_supported": False,
            "live_dry_run_allowed": False,
            "live_real_order_allowed": False,
            "fail_closed_reason": "promotion_extension_missing",
        },
        "promotion_interpretation": "research_only_not_live_eligible",
        "scenario_results": [],
    }

    summary = summarize_candidate_result(candidate, "summary")

    assert summary["strategy_runtime_capabilities"]["research_only"] is True
    assert summary["promotion_interpretation"] == "research_only_not_live_eligible"


def test_report_detail_index_excludes_compact_diagnostics() -> None:
    candidate = {
        "parameter_candidate_id": "candidate_001",
        "acceptance_gate_result": "FAIL",
        "validation_strategy_diagnostics": {"blocked_filter_distribution": {"x": 1}},
        "scenario_results": [],
    }

    summary = summarize_candidate_result(candidate, "index")

    assert summary["candidate_result_detail_policy"] == "index_bounded"
    assert "validation_strategy_diagnostics" not in summary


def test_report_detail_summary_includes_compact_diagnostics() -> None:
    candidate = {
        "parameter_candidate_id": "candidate_001",
        "validation_strategy_diagnostics": {"blocked_filter_distribution": {"x": 1}},
        "scenario_results": [],
    }

    summary = summarize_candidate_result(candidate, "summary")

    assert summary["candidate_result_detail_policy"] == "summary_bounded"
    assert summary["validation_strategy_diagnostics"]["blocked_filter_distribution"] == {"x": 1}


def test_report_detail_standard_includes_closed_trade_summary() -> None:
    candidate = {
        "parameter_candidate_id": "candidate_001",
        "scenario_results": [
            {
                "scenario_id": "scenario_001",
                "validation_closed_trades": [{"side": "SELL", "net_pnl": 1.0}],
            }
        ],
    }

    summary = summarize_candidate_result(candidate, "standard")

    scenario = summary["scenario_results"][0]
    assert summary["candidate_result_detail_policy"] == "standard_bounded"
    assert scenario["validation_closed_trade_summary"]["closed_trade_count"] == 1


def test_report_detail_full_includes_retained_decisions_when_limits_allow() -> None:
    candidate = {
        "parameter_candidate_id": "candidate_001",
        "decisions": [{"ts": 1}],
        "validation_resource_usage": {"stage_trace": [{"stage": "validation"}]},
    }

    full = summarize_candidate_result(candidate, "full")

    assert full["decisions"] == [{"ts": 1}]
    assert "stage_trace" not in full["validation_resource_usage"]
    assert full["validation_resource_usage"]["stage_trace_count"] == 1


def test_candidate_result_summary_omits_resource_usage_stage_trace() -> None:
    candidate = {
        "parameter_candidate_id": "candidate_001",
        "candidate_profile_hash": "sha256:profile",
        "retained_detail_summary": {"report_detail": "summary"},
        "scenario_results": [
            {
                "scenario_id": "scenario_001",
                "behavior_hash": "sha256:scenario-behavior",
                "equity_curve_hash": "sha256:equity",
                "retained_detail_summary": {"retained_equity_point_count": 0},
                "train_resource_usage": {
                    "behavior_hash": "sha256:train-behavior",
                    "equity_curve_hash": "sha256:train-equity",
                    "stage_trace": [{"stage": "train", "bar_index": 1}],
                    "stage_trace_hash": "sha256:train-stage-trace",
                    "decision_count": 1,
                    "memory_summary": {"peak_rss_mb": 128.0},
                },
                "validation_resource_usage": {
                    "behavior_hash": "sha256:validation-behavior",
                    "equity_curve_hash": "sha256:validation-equity",
                    "stage_trace": [{"stage": "validation", "bar_index": 1}],
                    "stage_trace_hash": "sha256:validation-stage-trace",
                    "trade_count": 1,
                },
                "final_holdout_resource_usage": {
                    "behavior_hash": "sha256:holdout-behavior",
                    "equity_curve_hash": "sha256:holdout-equity",
                    "stage_trace": [{"stage": "final_holdout", "bar_index": 1}],
                    "stage_trace_hash": "sha256:holdout-stage-trace",
                },
            }
        ],
    }

    summary = summarize_candidate_result(candidate, "summary")

    assert summary["candidate_profile_hash"] == "sha256:profile"
    assert summary["retained_detail_summary"] == {"report_detail": "summary"}
    scenario = summary["scenario_results"][0]
    assert scenario["retained_detail_summary"] == {"retained_equity_point_count": 0}
    for key, expected_hash in (
        ("train_resource_usage", "sha256:train-stage-trace"),
        ("validation_resource_usage", "sha256:validation-stage-trace"),
        ("final_holdout_resource_usage", "sha256:holdout-stage-trace"),
    ):
        usage = scenario[key]
        assert "stage_trace" not in usage
        assert usage["stage_trace_count"] == 1
        assert usage["stage_trace_hash"] == expected_hash
        assert usage["behavior_hash"].startswith("sha256:")
        assert usage["equity_curve_hash"].startswith("sha256:")
    assert scenario["train_resource_usage"]["decision_count"] == 1
    assert scenario["train_resource_usage"]["memory_summary"] == {"peak_rss_mb": 128.0}
    assert scenario["validation_resource_usage"]["trade_count"] == 1


def test_summary_derived_candidate_resource_usage_is_bounded() -> None:
    candidate = {
        "parameter_candidate_id": "candidate_001",
        "candidate_profile_hash": "sha256:profile",
        "candidate_behavior_profile_hash": "sha256:behavior-profile",
        "retained_detail_summary": {"report_detail": "summary"},
        "scenario_results": [
            {
                "scenario_id": "scenario_001",
                "train_resource_usage": {
                    "applied_resource_limits": {"max_trades": 1},
                    "behavior_hash": "sha256:train-behavior",
                    "canonical_hash_payload_bytes": 123_456,
                    "canonical_payload_hash_call_count": 10,
                    "equity_curve_hash": "sha256:train-equity",
                    "observability_policy": "summary_aggregate",
                    "resource_policy": {"max_decisions_retained": 0},
                    "nested": {"stage_trace": [{"stage": "nested"}]},
                    "stage_trace": [{"stage": "train"}],
                    "stage_trace_hash": "sha256:stage-trace",
                    "stage_trace_sample": [{"stage": "sample"}],
                    "stable_value_call_count": 20,
                    "tick_observability_policy": {"name": "summary_aggregate"},
                },
                "validation_execution_metadata": [{"ts": 1, "fill": "large"}],
                "validation_equity_curve": [{"ts": 1, "equity": 1.0}],
                "equity_curve_hash": "sha256:equity",
                "retained_detail_summary": {"retained_equity_point_count": 0},
            }
        ],
    }

    summary = summarize_derived_candidate(candidate, "summary")

    assert summary["derived_detail_policy"] == "summary_bounded"
    assert summary["candidate_profile_hash"] == "sha256:profile"
    assert summary["candidate_behavior_profile_hash"] == "sha256:behavior-profile"
    assert summary["retained_detail_summary"] == {"report_detail": "summary"}
    scenario = summary["scenario_results"][0]
    usage = scenario["train_resource_usage"]
    assert "stage_trace" not in usage
    assert "stage_trace" not in usage["nested"]
    assert "canonical_hash_payload_bytes" not in usage
    assert "canonical_payload_hash_call_count" not in usage
    assert "observability_policy" not in usage
    assert "applied_resource_limits_hash" not in usage
    assert "resource_policy_hash" not in usage
    assert "stable_value_call_count" not in usage
    assert "stage_trace_sample_count" not in usage
    assert "stage_trace_sample_hash" not in usage
    assert "tick_observability_policy" not in usage
    assert usage["stage_trace_hash"] == "sha256:stage-trace"
    assert usage["nested"]["stage_trace_count"] == 1
    assert scenario["validation_equity_curve"] == []
    assert "validation_execution_metadata" not in scenario
    assert scenario["retained_detail_summary"] == {"retained_equity_point_count": 0}


def test_summary_derived_candidate_compacts_large_retained_detail_summary() -> None:
    retained_detail_summary = {
        "report_detail": "summary",
        "decision_count": 120,
        "retained_decision_count": 0,
        "retained_equity_point_count": 0,
        "retained_regime_snapshot_count": 0,
        "decision_hash": "sha256:decision",
        "behavior_hash": "sha256:behavior",
        "trade_ledger_hash": "sha256:ledger",
        "equity_curve_hash": "sha256:equity",
    }
    candidate = {
        "parameter_candidate_id": "candidate_001",
        "retained_detail_summary": retained_detail_summary,
        "scenario_results": [
            {
                "scenario_id": "scenario_001",
                "retained_detail_summary": retained_detail_summary,
            }
        ],
    }

    summary = summarize_derived_candidate(candidate, "summary")

    compact = summary["retained_detail_summary"]
    assert compact["retained_detail_summary_hash"].startswith("sha256:")
    assert compact == {
        "retained_detail_summary_hash": compact["retained_detail_summary_hash"],
        "retained_detail_summary_key_count": 9,
        "report_detail": "summary",
        "decision_count": 120,
        "retained_decision_count": 0,
        "retained_equity_point_count": 0,
        "retained_regime_snapshot_count": 0,
    }
    scenario = summary["scenario_results"][0]
    assert scenario["retained_detail_summary"]["retained_detail_summary_hash"].startswith("sha256:")
    assert scenario["retained_detail_summary"]["decision_count"] == 120
    assert "decision_hash" not in scenario["retained_detail_summary"]
