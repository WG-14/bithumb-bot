from __future__ import annotations

from pathlib import Path

from scripts.collect_pytest_perf_baseline import (
    build_perf_baseline,
    parse_pytest_summary,
    validate_perf_baseline,
)


def _duration_file(path: Path) -> Path:
    path.write_text(
        """
============================= slowest durations =============================
52.49s call     tests/test_research_backtest_reproducibility.py::test_persisted_report_contains_report_write_stage_timing
10.00s setup    tests/test_orderbook_top_research.py::test_depth_walk_research_backtest_uses_signal_level_l2_depth
====================== 4570 passed in 576.56s ======================
""",
        encoding="utf-8",
    )
    return path


def test_perf_baseline_collector_parses_pytest_seconds(tmp_path: Path) -> None:
    seconds, count = parse_pytest_summary(_duration_file(tmp_path / "durations.txt").read_text(encoding="utf-8"))

    assert seconds == 576.56
    assert count == 4570


def test_perf_baseline_collector_records_xdist_settings(tmp_path: Path) -> None:
    baseline = build_perf_baseline(
        durations_file=_duration_file(tmp_path / "durations.txt"),
        xdist_workers=8,
        xdist_dist="worksteal",
    )

    assert baseline["xdist_workers"] == 8
    assert baseline["xdist_dist"] == "worksteal"
    assert baseline["top_duration_nodeids"][0]["nodeid"].endswith(
        "::test_persisted_report_contains_report_write_stage_timing"
    )


def test_perf_baseline_collector_includes_research_workload_summary(tmp_path: Path) -> None:
    baseline = build_perf_baseline(
        durations_file=_duration_file(tmp_path / "durations.txt"),
        xdist_workers=8,
        xdist_dist="worksteal",
    )

    assert baseline["expensive_test_count"] > 0
    assert "strategy_count" in baseline
    assert "manifest_count" in baseline
    assert "strategy_canary_count" in baseline
    assert "estimated_strategy_runs" in baseline
    assert validate_perf_baseline({"pytest_seconds": 576.56, "test_count": 4570}) == [
        "xdist_workers",
        "xdist_dist",
        "expensive_test_count",
        "strategy_count",
        "manifest_count",
        "strategy_canary_count",
        "estimated_strategy_runs",
        "estimated_tick_events",
        "estimated_audit_stream_rows",
        "top_duration_nodeids",
    ]
