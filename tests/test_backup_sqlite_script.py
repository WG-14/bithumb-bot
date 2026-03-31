from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
from pathlib import Path

import pytest


@pytest.mark.skipif(shutil.which("sqlite3") is None, reason="sqlite3 CLI is required")
def test_backup_script_uses_backup_root_mode_scoped_directory(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    scripts_dir = project_root / "scripts"
    data_dir = project_root / "data"
    scripts_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)

    source_script = Path("scripts/backup_sqlite.sh").resolve()
    script_path = scripts_dir / "backup_sqlite.sh"
    script_path.write_text(source_script.read_text(encoding="utf-8"), encoding="utf-8", newline="\n")
    source_path_query = Path("scripts/path_query.py").resolve()
    path_query_script = scripts_dir / "path_query.py"
    path_query_script.write_text(source_path_query.read_text(encoding="utf-8"), encoding="utf-8", newline="\n")

    db_path = data_dir / "sample.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE t(x INTEGER)")
        conn.execute("INSERT INTO t(x) VALUES (1)")
        conn.commit()
    finally:
        conn.close()

    first_cwd = tmp_path / "cwd-a"
    second_cwd = tmp_path / "cwd-b"
    first_cwd.mkdir()
    second_cwd.mkdir()

    env = {
        "DB_PATH": str(db_path.resolve()),
        "BACKUP_ROOT": str((project_root / "backup-root").resolve()),
        "MODE": "paper",
        "PYTHONPATH": str(Path("src").resolve()),
    }

    first = subprocess.run(
        ["bash", str(script_path)],
        cwd=first_cwd,
        env={**os.environ, **env},
        text=True,
        capture_output=True,
        check=False,
    )
    second = subprocess.run(
        ["bash", str(script_path)],
        cwd=second_cwd,
        env={**os.environ, **env},
        text=True,
        capture_output=True,
        check=False,
    )

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert "$'\\r'" not in first.stderr
    assert "$'\\r'" not in second.stderr

    backup_dir = project_root / "backup-root" / "paper" / "db"
    backups = sorted(backup_dir.glob("sample.sqlite.*.sqlite"))
    assert backups
    first_backup = first.stdout.strip().split()[-1]
    second_backup = second.stdout.strip().split()[-1]
    assert first_backup.startswith(str(backup_dir))
    assert second_backup.startswith(str(backup_dir))
    assert not (first_cwd / "backup-root").exists()
    assert not (second_cwd / "backup-root").exists()


@pytest.mark.skipif(shutil.which("sqlite3") is None, reason="sqlite3 CLI is required")
def test_backup_script_rejects_live_repo_internal_backup_override(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    scripts_dir = project_root / "scripts"
    data_dir = tmp_path / "runtime" / "data"
    scripts_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)

    source_script = Path("scripts/backup_sqlite.sh").resolve()
    script_path = scripts_dir / "backup_sqlite.sh"
    script_path.write_text(source_script.read_text(encoding="utf-8"), encoding="utf-8", newline="\n")
    source_path_query = Path("scripts/path_query.py").resolve()
    path_query_script = scripts_dir / "path_query.py"
    path_query_script.write_text(source_path_query.read_text(encoding="utf-8"), encoding="utf-8", newline="\n")

    db_path = data_dir / "live.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE t(x INTEGER)")
        conn.commit()
    finally:
        conn.close()

    env = {
        "MODE": "live",
        "ENV_ROOT": str((tmp_path / "runtime" / "env").resolve()),
        "RUN_ROOT": str((tmp_path / "runtime" / "run").resolve()),
        "DATA_ROOT": str((tmp_path / "runtime" / "data").resolve()),
        "LOG_ROOT": str((tmp_path / "runtime" / "logs").resolve()),
        "BACKUP_ROOT": str((tmp_path / "runtime" / "backup").resolve()),
        "DB_PATH": str(db_path.resolve()),
        "BACKUP_DIR": str((project_root / "backup").resolve()),
        "PYTHONPATH": str(Path("src").resolve()),
    }

    out = subprocess.run(
        ["bash", str(script_path)],
        cwd=tmp_path,
        env={**os.environ, **env},
        text=True,
        capture_output=True,
        check=False,
    )

    assert out.returncode != 0
    assert "BACKUP_DIR must be outside repository when MODE=live" in (out.stderr + out.stdout)
