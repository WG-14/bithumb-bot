#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def extract_stage_timing_summary(report_path: Path, *, require_stage_timings: bool = False) -> dict[str, Any]:
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    observability = payload.get("execution_observability")
    stage_timings = observability.get("stage_timings") if isinstance(observability, dict) else None
    if not isinstance(stage_timings, list):
        if require_stage_timings:
            raise ValueError("missing execution_observability.stage_timings")
        stage_timings = []
    stages: dict[str, float] = {}
    for item in stage_timings:
        if not isinstance(item, dict):
            continue
        stage = str(item.get("stage") or "").strip()
        if not stage:
            continue
        seconds = item.get("wall_seconds", item.get("duration_seconds", 0.0))
        try:
            stages[stage] = stages.get(stage, 0.0) + float(seconds or 0.0)
        except (TypeError, ValueError):
            continue
    dominant_stage = max(stages, key=stages.get) if stages else None
    return {
        "report": report_path.name,
        "total_stage_seconds": round(sum(stages.values()), 8),
        "stages": stages,
        "dominant_stage": dominant_stage,
    }


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--require-stage-timings", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        summary = extract_stage_timing_summary(
            args.report,
            require_stage_timings=args.require_stage_timings,
        )
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
