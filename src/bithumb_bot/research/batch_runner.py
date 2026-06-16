from __future__ import annotations

import glob
import subprocess
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bithumb_bot.paths import PathManager, PathPolicyError
from bithumb_bot.storage_io import write_json_atomic, write_text_atomic

from .experiment_manifest import load_manifest
from .report_writer import research_paths


@dataclass(frozen=True)
class ResearchBatchResult:
    summary_path: Path
    payload: dict[str, Any]


def run_research_batch(
    *,
    manifest_glob: str,
    max_concurrent_manifests: int,
    command: str,
    fail_fast: bool,
    out_path: str | Path | None,
    manager: PathManager,
    project_root: Path,
) -> ResearchBatchResult:
    if command != "research-backtest":
        raise ValueError("research_batch_supports_research_backtest_only")
    manifest_paths = [Path(path).expanduser().resolve() for path in sorted(glob.glob(str(manifest_glob)))]
    if not manifest_paths:
        raise ValueError("research_batch_manifest_glob_matched_no_files")
    manifests = [(path, load_manifest(path)) for path in manifest_paths]
    experiment_ids: dict[str, Path] = {}
    duplicates: list[str] = []
    for path, manifest in manifests:
        if manifest.experiment_id in experiment_ids:
            duplicates.append(manifest.experiment_id)
        experiment_ids[manifest.experiment_id] = path
    if duplicates:
        raise ValueError("research_batch_duplicate_experiment_ids:" + ",".join(sorted(set(duplicates))))

    max_concurrent = max(1, int(max_concurrent_manifests))
    summary_path = _batch_summary_path(manager=manager, out_path=out_path)
    batch_root = summary_path.parent / f"{summary_path.stem}_logs"
    _ensure_allowed(manager, batch_root)
    started = time.perf_counter()
    statuses: list[dict[str, Any]] = []
    failed = False

    def submit_one(path_manifest: tuple[Path, Any]) -> dict[str, Any]:
        path, manifest = path_manifest
        return _run_one_manifest(
            path=path,
            manifest=manifest,
            command=command,
            manager=manager,
            project_root=project_root,
            log_dir=batch_root,
        )

    remaining = iter(manifests)
    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        futures: dict[Any, tuple[Path, Any]] = {}

        def fill() -> None:
            while len(futures) < max_concurrent and not (failed and fail_fast):
                try:
                    item = next(remaining)
                except StopIteration:
                    return
                futures[executor.submit(submit_one, item)] = item

        fill()
        while futures:
            done, _ = wait(tuple(futures), return_when=FIRST_COMPLETED)
            for future in done:
                futures.pop(future)
                status = future.result()
                statuses.append(status)
                if status["status"] != "succeeded":
                    failed = True
            fill()

    payload = {
        "schema_version": 1,
        "artifact_type": "research_batch_summary",
        "command": command,
        "manifest_glob": manifest_glob,
        "max_concurrent_manifests": max_concurrent,
        "fail_fast": bool(fail_fast),
        "process_model": "subprocess",
        "status": "failed" if any(item["status"] != "succeeded" for item in statuses) else "succeeded",
        "elapsed_s": round(time.perf_counter() - started, 6),
        "manifest_count": len(manifests),
        "manifests": sorted(statuses, key=lambda item: item["manifest_path"]),
    }
    write_json_atomic(summary_path, payload)
    return ResearchBatchResult(summary_path=summary_path, payload=payload)


def _run_one_manifest(
    *,
    path: Path,
    manifest: Any,
    command: str,
    manager: PathManager,
    project_root: Path,
    log_dir: Path,
) -> dict[str, Any]:
    started = time.perf_counter()
    report_path = research_paths(manager, manifest.experiment_id, "backtest").report_path
    log_path = log_dir / f"{_safe_name(manifest.experiment_id)}.log"
    _ensure_allowed(manager, log_path)
    cmd = [
        sys.executable,
        "-m",
        "bithumb_bot",
        command,
        "--manifest",
        str(path),
        "--notification-policy",
        "disabled",
    ]
    completed = subprocess.run(
        cmd,
        cwd=str(project_root),
        text=True,
        capture_output=True,
        check=False,
    )
    write_text_atomic(
        log_path,
        "COMMAND " + " ".join(cmd) + "\n\nSTDOUT\n" + completed.stdout + "\nSTDERR\n" + completed.stderr,
    )
    return {
        "manifest_path": str(path),
        "experiment_id": manifest.experiment_id,
        "status": "succeeded" if completed.returncode == 0 else "failed",
        "exit_code": int(completed.returncode),
        "elapsed_s": round(time.perf_counter() - started, 6),
        "report_path": str(report_path.resolve()),
        "log_path": str(log_path.resolve()),
        "failure_reason": None if completed.returncode == 0 else "subprocess_exit_nonzero",
    }


def _batch_summary_path(*, manager: PathManager, out_path: str | Path | None) -> Path:
    if out_path is None:
        path = manager.data_dir() / "reports" / "research" / "batch" / "research_batch_summary.json"
    else:
        path = Path(out_path).expanduser()
        if not path.is_absolute():
            path = manager.data_dir() / "reports" / "research" / "batch" / path
    resolved = path.resolve()
    _ensure_allowed(manager, resolved)
    return resolved


def _ensure_allowed(manager: PathManager, path: Path) -> None:
    resolved = path.resolve()
    if PathManager._is_within(resolved, manager.project_root.resolve()):
        raise PathPolicyError(f"research batch artifact must be outside repository: {resolved}")
    if not PathManager._is_within(resolved, manager.data_dir().resolve()):
        raise PathPolicyError(f"research batch artifact must be under DATA_ROOT: {resolved}")


def _safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(value or "unknown"))[:128]
