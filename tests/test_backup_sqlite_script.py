from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
from pathlib import Path

import pytest


@pytest.mark.skipif(shutil.which("sqlite3") is None, reason="sqlite3 CLI is required")
def test_backup_script_resolves_relative_paths_from_project_root(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    scripts_dir = project_root / "scripts"
    data_dir = project_root / "data"
    scripts_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)

    source_script = Path("scripts/backup_sqlite.sh").resolve()
    script_path = scripts_dir / "backup_sqlite.sh"
    script_path.write_text(source_script.read_text(encoding="utf-8"), encoding="utf-8", newline="\n")

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
        "DB_PATH": "data/sample.sqlite",
        "BACKUP_DIR": "backups",
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

    backup_dir = project_root / "backups"
    backups = sorted(backup_dir.glob("sample.sqlite.*.sqlite"))
    assert backups
    first_backup = first.stdout.strip().split()[-1]
    second_backup = second.stdout.strip().split()[-1]
    assert first_backup.startswith(str(backup_dir))
    assert second_backup.startswith(str(backup_dir))
    assert not (first_cwd / "backups").exists()
    assert not (second_cwd / "backups").exists()
