from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _managed_env(tmp_path: Path, mode: str) -> dict[str, str]:
    env = dict(os.environ)
    env["MODE"] = mode
    env["PYTHONPATH"] = "src"
    env["ENV_ROOT"] = str((tmp_path / "env").resolve())
    env["RUN_ROOT"] = str((tmp_path / "run").resolve())
    env["DATA_ROOT"] = str((tmp_path / "data").resolve())
    env["LOG_ROOT"] = str((tmp_path / "logs").resolve())
    env["BACKUP_ROOT"] = str((tmp_path / "backup").resolve())
    return env


def _run_path_cli(tmp_path: Path, env: dict[str, str], kind: str, mode: str) -> Path:
    out = subprocess.run(
        [
            sys.executable,
            "-m",
            "bithumb_bot.paths",
            "--project-root",
            str((tmp_path / "repo").resolve()),
            "--mode",
            mode,
            "--kind",
            kind,
        ],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert out.returncode == 0, out.stderr
    return Path(out.stdout.strip())


def test_paths_cli_keeps_mode_scoped_directories(tmp_path: Path) -> None:
    env = _managed_env(tmp_path, mode="paper")
    paper_db = _run_path_cli(tmp_path, env, "primary-db", "paper")
    live_db = _run_path_cli(tmp_path, env, "primary-db", "live")
    paper_snapshot = _run_path_cli(tmp_path, env, "backup-snapshots-dir", "paper")
    live_snapshot = _run_path_cli(tmp_path, env, "backup-snapshots-dir", "live")
    paper_report = _run_path_cli(tmp_path, env, "reports-ops-dir", "paper")
    live_report = _run_path_cli(tmp_path, env, "reports-ops-dir", "live")

    assert paper_db == Path(env["DATA_ROOT"]) / "paper" / "trades" / "paper.sqlite"
    assert live_db == Path(env["DATA_ROOT"]) / "live" / "trades" / "live.sqlite"
    assert paper_snapshot == Path(env["BACKUP_ROOT"]) / "paper" / "snapshots"
    assert live_snapshot == Path(env["BACKUP_ROOT"]) / "live" / "snapshots"
    assert paper_report == Path(env["DATA_ROOT"]) / "paper" / "reports" / "ops"
    assert live_report == Path(env["DATA_ROOT"]) / "live" / "reports" / "ops"


def test_operational_scripts_use_path_manager_queries() -> None:
    scripts = {
        "scripts/check_live_runtime.sh": "backup-snapshots-dir",
        "scripts/collect_live_snapshot.sh": "backup-snapshots-dir",
        "scripts/backup_sqlite.sh": "backup-db-dir",
    }
    legacy_patterns = (
        "data/live.sqlite",
        "data/locks/",
        "ls -1t backups",
        "snapshots/live_",
    )

    for script_path, expected_kind in scripts.items():
        content = Path(script_path).read_text(encoding="utf-8")
        assert "python3 -m bithumb_bot.paths" in content
        assert expected_kind in content
        for legacy in legacy_patterns:
            assert legacy not in content
