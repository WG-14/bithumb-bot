from __future__ import annotations

from bithumb_bot.research.cli import _print_report_summary, _print_research_backtest_progress
from bithumb_bot.research.run_summary import build_research_run_summary


def _candidate(
    candidate_id: str,
    *,
    gate: str = "FAIL",
    fail_reasons: list[str] | None = None,
    walk_forward_metrics: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "parameter_candidate_id": candidate_id,
        "acceptance_gate_result": gate,
        "gate_fail_reasons": fail_reasons or [],
    }
    if walk_forward_metrics is not None:
        payload["walk_forward_metrics"] = walk_forward_metrics
    return payload


def _report(*, candidates: object, best_candidate_id: str | None = None, gate_result: str = "FAIL") -> dict[str, object]:
    return {
        "experiment_id": "summary_exp",
        "manifest_hash": "sha256:manifest",
        "dataset_snapshot_id": "snapshot",
        "dataset_content_hash": "sha256:dataset",
        "candidate_count": len(candidates) if isinstance(candidates, list) else 0,
        "best_candidate_id": best_candidate_id,
        "gate_result": gate_result,
        "artifact_paths": {
            "report_path": "/tmp/reports/research/summary_exp/backtest_report.json",
            "derived_path": "/tmp/derived/research/summary_exp/backtest_candidates.json",
        },
        "content_hash": "sha256:report",
        "warnings": [],
        "candidates": candidates,
    }


def test_fail_report_summary_includes_top_fail_reasons() -> None:
    summary = build_research_run_summary(
        _report(
            candidates=[
                _candidate("candidate_001", fail_reasons=["profit_factor_failed", "walk_forward_failed"]),
                _candidate("candidate_002", fail_reasons=["profit_factor_failed", "min_trade_count_failed"]),
            ]
        )
    )

    assert summary.top_fail_reasons == {
        "profit_factor_failed": 2,
        "min_trade_count_failed": 1,
        "walk_forward_failed": 1,
    }


def test_pass_report_summary_sets_promotion_allowed() -> None:
    summary = build_research_run_summary(
        _report(
            candidates=[_candidate("candidate_001", gate="PASS")],
            best_candidate_id="candidate_001",
            gate_result="PASS",
        )
    )

    assert summary.promotion_allowed is True
    assert summary.next_action == "review_promotion_candidate"


def test_statistical_gate_failure_forces_promotion_disallowed() -> None:
    report = _report(
        candidates=[_candidate("candidate_001", gate="PASS")],
        best_candidate_id="candidate_001",
        gate_result="PASS",
    )
    report.update(
        {
            "statistical_validation_required": True,
            "statistical_gate_result": "FAIL",
            "statistical_gate_fail_reasons": ["reality_check_p_value_failed"],
        }
    )

    summary = build_research_run_summary(report)

    assert summary.promotion_allowed is False
    assert summary.next_action == "do_not_promote_review_statistical_selection"


def test_fail_report_summary_sets_promotion_disallowed() -> None:
    summary = build_research_run_summary(
        _report(candidates=[_candidate("candidate_001", fail_reasons=["profit_factor_failed"])])
    )

    assert summary.promotion_allowed is False


def test_candidate_gate_counts_are_computed_with_unknown_labels() -> None:
    summary = build_research_run_summary(
        _report(
            candidates=[
                _candidate("candidate_001", gate="PASS"),
                _candidate("candidate_002", gate="FAIL"),
                _candidate("candidate_003", gate="REVIEW"),
                {"parameter_candidate_id": "candidate_004"},
            ]
        )
    )

    assert summary.candidate_gate_counts == {"PASS": 1, "FAIL": 1, "REVIEW": 1, "UNKNOWN": 1}


def test_walk_forward_report_summary_includes_window_summary() -> None:
    summary = build_research_run_summary(
        _report(
            candidates=[
                _candidate(
                    "candidate_001",
                    fail_reasons=["walk_forward_failed"],
                    walk_forward_metrics={
                        "window_count": 5,
                        "pass_window_count": 1,
                        "fail_window_count": 4,
                        "windows": [],
                    },
                )
            ]
        )
    )

    assert summary.walk_forward_window_count == 5
    assert summary.walk_forward_pass_window_count == 1
    assert summary.walk_forward_fail_window_count == 4


def test_walk_forward_report_summary_includes_top_window_fail_reasons() -> None:
    summary = build_research_run_summary(
        _report(
            candidates=[
                _candidate(
                    "candidate_001",
                    fail_reasons=["walk_forward_failed"],
                    walk_forward_metrics={
                        "window_count": 2,
                        "pass_window_count": 0,
                        "fail_window_count": 2,
                        "windows": [
                            {"gate_result": "FAIL", "fail_reasons": ["test_metrics_gate_incompatible"]},
                            {
                                "gate_result": "FAIL",
                                "fail_reasons": ["test_metrics_gate_incompatible", "test_return_not_positive"],
                            },
                        ],
                    },
                )
            ]
        )
    )

    assert summary.top_window_fail_reasons == {
        "test_metrics_gate_incompatible": 2,
        "test_return_not_positive": 1,
    }


def test_empty_or_malformed_candidate_sections_do_not_crash() -> None:
    empty_summary = build_research_run_summary(_report(candidates=[]))
    malformed_summary = build_research_run_summary(_report(candidates={"not": "a-list"}))
    mixed_summary = build_research_run_summary(_report(candidates=["bad-row", {"gate_fail_reasons": "bad"}]))

    assert empty_summary.candidate_gate_counts == {}
    assert empty_summary.next_action == "inspect_dataset_or_manifest"
    assert malformed_summary.candidate_gate_counts == {}
    assert mixed_summary.candidate_gate_counts == {"PASS": 0, "FAIL": 0, "UNKNOWN": 1}


def test_best_candidate_remains_none_when_all_candidates_fail() -> None:
    report = _report(candidates=[_candidate("candidate_001", fail_reasons=["profit_factor_failed"])])
    summary = build_research_run_summary(report)

    assert report["best_candidate_id"] is None
    assert summary.promotion_allowed is False


def test_nearest_failed_candidate_is_separate_from_best_candidate() -> None:
    report = _report(
        candidates=[
            _candidate("candidate_004_1e322c70", fail_reasons=["profit_factor_failed", "walk_forward_missing"])
        ]
    )
    summary = build_research_run_summary(report)

    assert report["best_candidate_id"] is None
    assert summary.nearest_failed_candidate_id == "candidate_004_1e322c70"
    assert summary.nearest_failed_candidate_fail_reasons == ("profit_factor_failed", "walk_forward_missing")


def test_next_action_is_conservative_for_failed_reports() -> None:
    walk_missing = build_research_run_summary(
        _report(candidates=[_candidate("candidate_001", fail_reasons=["walk_forward_missing"])])
    )
    walk_failed = build_research_run_summary(
        _report(candidates=[_candidate("candidate_001", fail_reasons=["walk_forward_failed"])])
    )
    strategy_failed = build_research_run_summary(
        _report(candidates=[_candidate("candidate_001", fail_reasons=["profit_factor_failed"])])
    )

    assert walk_missing.next_action == "run_walk_forward_before_promotion"
    assert walk_failed.next_action == "do_not_promote_review_walk_forward_windows"
    assert strategy_failed.next_action == "do_not_promote_revise_strategy_hypothesis"
    assert strategy_failed.next_action != "review_promotion_candidate"


def test_print_report_summary_renders_operator_diagnostics(capsys) -> None:
    report = _report(
        candidates=[
            _candidate(
                "candidate_004_1e322c70",
                fail_reasons=["profit_factor_failed", "walk_forward_failed"],
                walk_forward_metrics={
                    "window_count": 5,
                    "pass_window_count": 0,
                    "fail_window_count": 5,
                    "windows": [
                        {"gate_result": "FAIL", "fail_reasons": ["test_metrics_gate_incompatible"]},
                    ],
                },
            )
        ]
    )

    _print_report_summary("RESEARCH-WALK-FORWARD", report)

    output = capsys.readouterr().out
    assert "candidate_gate_counts=PASS:0,FAIL:1" in output
    assert "top_fail_reasons=profit_factor_failed:1,walk_forward_failed:1" in output
    assert "promotion_allowed=0" in output
    assert "best_candidate_id=none" in output
    assert "nearest_failed_candidate_id=candidate_004_1e322c70" in output
    assert "walk_forward_window_summary=window_count:5,pass:0,fail:5" in output
    assert "top_window_fail_reasons=test_metrics_gate_incompatible:1" in output
    assert "next_action=do_not_promote_review_walk_forward_windows" in output


def test_print_report_summary_renders_statistical_selection_diagnostics(capsys) -> None:
    report = _report(
        candidates=[_candidate("candidate_001", gate="PASS")],
        best_candidate_id="candidate_001",
        gate_result="PASS",
    )
    report.update(
        {
            "statistical_validation_required": True,
            "parameter_grid_size": 5000,
            "search_budget": 5000,
            "attempt_index": 3,
            "holdout_reuse_count": 2,
            "selection_universe_hash": "sha256:selection",
            "statistical_evidence_hash": "sha256:evidence",
            "white_reality_check_p_value": 0.2,
            "statistical_gate_result": "FAIL",
            "statistical_gate_fail_reasons": ["reality_check_p_value_failed"],
        }
    )

    _print_report_summary("RESEARCH-BACKTEST", report)

    output = capsys.readouterr().out
    assert "statistical_validation_required=1" in output
    assert "statistical_parameter_grid_size=5000" in output
    assert "statistical_search_budget=5000" in output
    assert "statistical_attempt_index=3" in output
    assert "statistical_holdout_reuse_count=2" in output
    assert "selection_universe_hash=sha256:selection" in output
    assert "statistical_evidence_hash=sha256:evidence" in output
    assert "white_reality_check_p_value=0.2" in output
    assert "statistical_gate_result=FAIL" in output
    assert "statistical_gate_fail_reasons=reality_check_p_value_failed" in output
    assert "promotion_allowed=0" in output
    assert "next_action=do_not_promote_review_statistical_selection" in output


def test_print_report_summary_renders_stress_suite_diagnostics(capsys) -> None:
    report = _report(
        candidates=[_candidate("candidate_001", gate="FAIL")],
        best_candidate_id=None,
        gate_result="FAIL",
    )
    report.update(
        {
            "stress_suite_required": True,
            "stress_suite_gate_result": "FAIL",
            "stress_suite_fail_reasons": ["stress_trade_removal_return_retention_failed"],
            "best_validation_stress_suite": {
                "trade_removal": {"status": "FAIL"},
                "trade_order_monte_carlo": {
                    "survival_probability": 0.972,
                    "max_drawdown_pct_p95": 31.2,
                },
            },
        }
    )

    _print_report_summary("RESEARCH-BACKTEST", report)

    output = capsys.readouterr().out
    assert "stress_suite_required=1" in output
    assert "stress_suite_gate_result=FAIL" in output
    assert "stress_suite_fail_reasons=stress_trade_removal_return_retention_failed" in output
    assert "stress_trade_removal_status=FAIL" in output
    assert "stress_monte_carlo_survival_probability=0.972" in output
    assert "stress_monte_carlo_max_drawdown_pct_p95=31.2" in output


def test_print_report_summary_handles_missing_optional_stress_suite(capsys) -> None:
    _print_report_summary("RESEARCH-BACKTEST", _report(candidates=[]))

    output = capsys.readouterr().out
    assert "stress_suite_required=0" in output
    assert "stress_suite_gate_result=none" in output
    assert "stress_trade_removal_status=none" in output


def test_print_report_summary_renders_metrics_v2_for_passing_candidate(capsys) -> None:
    report = _report(
        candidates=[
            {
                **_candidate("candidate_001", gate="PASS"),
                "validation_metrics_v2": {
                    "metrics_schema_version": 2,
                    "return_risk": {"cagr_pct": 12.5, "open_position_at_end": False},
                    "trade_quality": {"expectancy_per_trade_krw": 250.0},
                    "time_exposure": {"exposure_time_pct": 25.0, "avg_holding_time_ms": 600000.0},
                    "cost_execution": {
                        "fee_drag_ratio": 0.001,
                        "fee_drag_ratio_basis": "traded_notional",
                        "slippage_drag_ratio": 0.002,
                        "slippage_drag_ratio_basis": "traded_notional",
                    },
                },
            }
        ],
        best_candidate_id="candidate_001",
        gate_result="PASS",
    )

    _print_report_summary("RESEARCH-BACKTEST", report)

    output = capsys.readouterr().out
    assert "metrics_v2_summary=schema=2 cagr_pct=12.5 expectancy_per_trade_krw=250.0" in output
    assert "exposure_time_pct=25.0 avg_holding_time_ms=600000.0 open_position_at_end=False" in output
    assert "fee_drag_ratio=0.001 fee_drag_ratio_basis=traded_notional" in output
    assert "slippage_drag_ratio=0.002 slippage_drag_ratio_basis=traded_notional" in output


def test_print_report_summary_renders_top_of_book_warning_context(capsys) -> None:
    report = _report(
        candidates=[
            _candidate(
                "candidate_001",
                gate="PASS",
                fail_reasons=[],
            )
        ],
        best_candidate_id="candidate_001",
        gate_result="PASS",
    )
    report["warnings"] = ["top_of_book_optional_coverage_warning"]
    report["top_of_book_quality_summary"] = {
        "requested": True,
        "required": False,
        "gate_status": "WARN",
        "coverage_pct": 50.0,
        "joined_quote_count": 10,
        "missing_quote_count": 10,
        "join_tolerance_ms": 3000,
        "affected_splits": [
            {"split_name": "validation", "top_of_book_missing_count": 10},
        ],
        "next_action": (
            "collect orderbook top snapshots with sync-orderbook-top, rerun research-backtest, "
            "and verify top_of_book_coverage_pct"
        ),
    }

    _print_report_summary("RESEARCH-BACKTEST", report)

    output = capsys.readouterr().out
    assert "warnings=top_of_book_optional_coverage_warning" in output
    assert "top_of_book_quote_coverage=requested=1 required=0 gate_status=WARN coverage_pct=50.0" in output
    assert "joined_count=10 missing_count=10 join_tolerance_ms=3000 affected_splits=validation" in output
    assert (
        "top_of_book_limitations=best_bid_ask_only_not_full_depth,"
        "intra_candle_path_unavailable"
    ) in output
    assert (
        "top_of_book_next_action=collect orderbook top snapshots with sync-orderbook-top, "
        "rerun research-backtest, and verify top_of_book_coverage_pct"
    ) in output


def test_research_backtest_progress_lines_are_operator_visible(capsys) -> None:
    _print_research_backtest_progress(
        {
            "stage": "start",
            "manifest_hash": "sha256:manifest",
            "db_path": "/runtime/data/paper/trades/paper.sqlite",
        }
    )
    _print_research_backtest_progress({"stage": "load_split", "split": "train", "candles": 4320})
    _print_research_backtest_progress(
        {
            "stage": "evaluate",
            "scenario": "1/1",
            "candidate": "1/1",
            "split": "validation",
            "candles": 4297,
        }
    )
    _print_research_backtest_progress({"stage": "report_write", "experiment_id": "summary_exp"})
    _print_research_backtest_progress({"stage": "complete", "experiment_id": "summary_exp"})

    output = capsys.readouterr().out
    assert "[RESEARCH-BACKTEST] stage=start" in output
    assert "stage=load_split" in output
    assert "stage=evaluate" in output
    assert "stage=report_write" in output
    assert "stage=complete" in output
