from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bithumb_bot.paths import PathManager, PathPolicyError
from bithumb_bot.storage_io import write_json_atomic

from .hashing import content_hash_payload, sha256_prefixed


@dataclass(frozen=True)
class ResearchReportPaths:
    derived_path: Path
    report_path: Path


def research_paths(manager: PathManager, experiment_id: str, report_name: str) -> ResearchReportPaths:
    derived_path = manager.data_dir() / "derived" / "research" / experiment_id / f"{report_name}_candidates.json"
    report_path = manager.data_dir() / "reports" / "research" / experiment_id / f"{report_name}_report.json"
    _ensure_research_output_path_allowed(manager, derived_path)
    _ensure_research_output_path_allowed(manager, report_path)
    return ResearchReportPaths(derived_path=derived_path, report_path=report_path)


def write_research_report(
    *,
    manager: PathManager,
    experiment_id: str,
    report_name: str,
    payload: dict[str, Any],
) -> tuple[ResearchReportPaths, str]:
    paths = research_paths(manager, experiment_id, report_name)
    content_hash = sha256_prefixed(content_hash_payload(payload))
    report_payload = dict(payload)
    report_payload["content_hash"] = content_hash
    write_json_atomic(paths.derived_path, {"candidates": report_payload.get("candidates", [])})
    write_json_atomic(paths.report_path, report_payload)
    return paths, content_hash


def _ensure_research_output_path_allowed(manager: PathManager, path: Path) -> None:
    project_root = manager.project_root.resolve()
    resolved = path.resolve()
    if PathManager._is_within(resolved, project_root):
        raise PathPolicyError(f"research output path must be outside repository: {resolved}")
