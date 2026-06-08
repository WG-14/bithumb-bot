from __future__ import annotations

import json
from pathlib import Path

from scripts.extract_research_stage_timings import extract_stage_timing_summary, main


def test_extract_stage_timings_from_persisted_report(tmp_path: Path) -> None:
    report = tmp_path / "backtest_report.json"
    report.write_text(
        json.dumps(
            {
                "execution_observability": {
                    "stage_timings": [
                        {"stage": "load_split", "wall_seconds": 1.0},
                        {"stage": "report_write", "wall_seconds": 2.0},
                    ]
                }
            }
        ),
        encoding="utf-8",
    )

    summary = extract_stage_timing_summary(report, require_stage_timings=True)

    assert summary["report"] == "backtest_report.json"
    assert summary["total_stage_seconds"] == 3.0
    assert summary["stages"]["load_split"] == 1.0


def test_extract_stage_timings_reports_dominant_stage(tmp_path: Path) -> None:
    report = tmp_path / "backtest_report.json"
    report.write_text(
        json.dumps(
            {
                "execution_observability": {
                    "stage_timings": [
                        {"stage": "load_split", "wall_seconds": 1.0},
                        {"stage": "candidate_evaluation", "wall_seconds": 10.0},
                        {"stage": "report_write", "wall_seconds": 2.0},
                    ]
                }
            }
        ),
        encoding="utf-8",
    )

    summary = extract_stage_timing_summary(report, require_stage_timings=True)

    assert summary["dominant_stage"] == "candidate_evaluation"


def test_extract_stage_timings_rejects_missing_stage_timings_in_strict_mode(tmp_path: Path) -> None:
    report = tmp_path / "backtest_report.json"
    report.write_text(json.dumps({"execution_observability": {}}), encoding="utf-8")

    assert main(["--report", str(report), "--require-stage-timings"]) == 1
