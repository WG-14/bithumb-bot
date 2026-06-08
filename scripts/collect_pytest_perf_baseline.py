#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.check_pytest_duration_inventory import parse_pytest_duration_file
from tests.policy.research_runner_policy import research_workload_summary


PYTEST_SUMMARY_RE = re.compile(r"=+\s*(?P<count>\d+)\s+passed.*?in\s+(?P<seconds>\d+(?:\.\d+)?)s\s*=+")
REQUIRED_BASELINE_FIELDS = (
    "pytest_seconds",
    "test_count",
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
)


def parse_pytest_summary(text: str) -> tuple[float | None, int | None]:
    for line in reversed(text.splitlines()):
        match = PYTEST_SUMMARY_RE.search(line)
        if match:
            return float(match.group("seconds")), int(match.group("count"))
    return None, None


def build_perf_baseline(
    *,
    durations_file: Path,
    xdist_workers: int,
    xdist_dist: str,
    test_root: Path = Path("tests"),
    inventory_path: Path = Path("tests/policy/research_e2e_inventory.json"),
) -> dict[str, Any]:
    text = durations_file.read_text(encoding="utf-8")
    pytest_seconds, test_count = parse_pytest_summary(text)
    durations, _ = parse_pytest_duration_file(durations_file)
    workload = research_workload_summary(test_root=test_root, inventory_path=inventory_path)
    top_duration_nodeids = [
        {"nodeid": row.nodeid, "phase": row.phase, "seconds": row.actual_seconds}
        for row in sorted(durations, key=lambda row: row.actual_seconds, reverse=True)[:20]
    ]
    return {
        "schema_version": 1,
        "duration_source": durations_file.as_posix(),
        "pytest_seconds": pytest_seconds,
        "test_count": test_count,
        "xdist_workers": int(xdist_workers),
        "xdist_dist": str(xdist_dist),
        "expensive_test_count": workload["expensive_test_count"],
        "strategy_count": workload["strategy_count"],
        "manifest_count": workload["manifest_count"],
        "strategy_canary_count": workload["strategy_canary_count"],
        "estimated_strategy_runs": workload["total_estimated_strategy_runs"],
        "estimated_tick_events": workload["total_estimated_tick_events"],
        "estimated_audit_stream_rows": workload["total_estimated_audit_stream_rows"],
        "top_duration_nodeids": top_duration_nodeids,
    }


def validate_perf_baseline(payload: dict[str, Any]) -> list[str]:
    missing = [field for field in REQUIRED_BASELINE_FIELDS if field not in payload]
    invalid: list[str] = []
    for field in ("pytest_seconds", "test_count", "xdist_workers", "xdist_dist"):
        if field not in missing and payload.get(field) in (None, ""):
            invalid.append(field)
    if "top_duration_nodeids" not in missing and not isinstance(payload.get("top_duration_nodeids"), list):
        invalid.append("top_duration_nodeids")
    return missing + invalid


def write_perf_baseline(
    *,
    durations_file: Path,
    output_dir: Path,
    timestamp: str,
    xdist_workers: int,
    xdist_dist: str,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    durations_out = output_dir / f"full_durations_{timestamp}.txt"
    workload_out = output_dir / f"research_workload_summary_{timestamp}.txt"
    baseline_out = output_dir / f"perf_baseline_{timestamp}.json"
    durations_out.write_text(durations_file.read_text(encoding="utf-8"), encoding="utf-8")
    workload = research_workload_summary()
    workload_out.write_text(json.dumps(workload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    baseline = build_perf_baseline(
        durations_file=durations_out,
        xdist_workers=xdist_workers,
        xdist_dist=xdist_dist,
    )
    violations = validate_perf_baseline(baseline)
    if violations:
        raise ValueError(f"incomplete perf baseline: {', '.join(violations)}")
    baseline_out.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return baseline_out


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--durations-file", type=Path, default=Path(".diagnostics/perf/full_durations_baseline.txt"))
    parser.add_argument("--output-dir", type=Path, default=Path(".diagnostics/perf"))
    parser.add_argument("--timestamp")
    parser.add_argument("--xdist-workers", type=int, default=int(os.environ.get("PYTEST_XDIST_WORKERS", "0") or 0))
    parser.add_argument("--xdist-dist", default=os.environ.get("PYTEST_XDIST_DIST", "unknown"))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    if not args.durations_file.exists():
        print(f"perf baseline collector requires existing duration file: {args.durations_file}", file=sys.stderr)
        return 1
    timestamp = args.timestamp or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    try:
        baseline_path = write_perf_baseline(
            durations_file=args.durations_file,
            output_dir=args.output_dir,
            timestamp=timestamp,
            xdist_workers=args.xdist_workers,
            xdist_dist=args.xdist_dist,
        )
    except (OSError, ValueError, json.JSONDecodeError, AssertionError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(baseline_path.as_posix())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
