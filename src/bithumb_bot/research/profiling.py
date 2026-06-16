from __future__ import annotations

import cProfile
import io
import pstats
import time
from pathlib import Path
from typing import Any, Callable, TypeVar

from bithumb_bot.paths import PathManager, PathPolicyError
from bithumb_bot.storage_io import write_json_atomic


T = TypeVar("T")


def run_with_cprofile(
    *,
    func: Callable[[], T],
    manager: PathManager,
    experiment_id: str,
    candidate_id: str,
    scenario_id: str,
    split_name: str,
    candles_processed: int,
) -> tuple[T, dict[str, Any]]:
    path = profile_artifact_path(
        manager=manager,
        experiment_id=experiment_id,
        candidate_id=candidate_id,
        scenario_id=scenario_id,
        split_name=split_name,
    )
    profiler = cProfile.Profile()
    started = time.perf_counter()
    result = profiler.runcall(func)
    wall_seconds = time.perf_counter() - started
    stats_stream = io.StringIO()
    pstats.Stats(profiler, stream=stats_stream).sort_stats("cumtime").print_stats(20)
    hotspots = _hotspots(profiler)
    payload = {
        "schema_version": 1,
        "artifact_type": "research_backtest_profile",
        "candidate_id": candidate_id,
        "scenario_id": scenario_id,
        "split_name": split_name,
        "candles_processed": int(candles_processed),
        "wall_seconds_total": round(wall_seconds, 6),
        "wall_seconds_by_stage": {
            "strategy_runner": round(wall_seconds, 6),
        },
        "top_hotspots": hotspots,
        "cprofile_text": stats_stream.getvalue(),
        "diagnostic_only": True,
        "promotion_evidence": False,
    }
    write_json_atomic(path, payload)
    return result, {
        "profile_artifact_path": str(path.resolve()),
        "profile_artifact_ref": path.resolve().relative_to(manager.data_dir().resolve()).as_posix(),
        "profile_artifact_type": "research_backtest_profile",
        "profile_artifact_written": True,
    }


def profile_artifact_path(
    *,
    manager: PathManager,
    experiment_id: str,
    candidate_id: str,
    scenario_id: str,
    split_name: str,
) -> Path:
    safe_name = "_".join(_safe_part(part) for part in (candidate_id, scenario_id, split_name))
    path = manager.data_dir() / "derived" / "research" / experiment_id / "profiles" / f"{safe_name}.json"
    resolved = path.resolve()
    data_dir = manager.data_dir().resolve()
    if PathManager._is_within(resolved, manager.project_root.resolve()):
        raise PathPolicyError(f"profile artifact must be outside repository: {resolved}")
    if not PathManager._is_within(resolved, data_dir):
        raise PathPolicyError(f"profile artifact must be inside DATA_ROOT derived bucket: {resolved}")
    return resolved


def _safe_part(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(value or "unknown"))
    return cleaned[:96] or "unknown"


def _hotspots(profiler: cProfile.Profile) -> list[dict[str, Any]]:
    stats = pstats.Stats(profiler)
    rows: list[dict[str, Any]] = []
    for func, stat in sorted(stats.stats.items(), key=lambda item: item[1][3], reverse=True)[:10]:
        cc, nc, tt, ct, _callers = stat
        filename, line_no, function_name = func
        rows.append(
            {
                "function": function_name,
                "filename": filename,
                "line_no": int(line_no),
                "primitive_calls": int(cc),
                "total_calls": int(nc),
                "total_seconds": round(float(tt), 6),
                "cumulative_seconds": round(float(ct), 6),
            }
        )
    return rows
