from __future__ import annotations

import json
from pathlib import Path

from bithumb_bot.research.report_writer import (
    ResearchReportPaths,
    persist_final_research_report_observability,
)


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
