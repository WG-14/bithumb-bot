from __future__ import annotations

from bithumb_bot.research.cli import _print_report_summary
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
