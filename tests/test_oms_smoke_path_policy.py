from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path


def _run_oms_smoke(*, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "tools/oms_smoke.py"],
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )


def test_managed_runtime_env_fixture_uses_non_repo_tmp_path(managed_runtime_env: dict[str, str]) -> None:
    project_root = Path(managed_runtime_env["project_root"]).resolve()
    runtime_root = Path(managed_runtime_env["runtime_root"]).resolve()
    db_path = Path(managed_runtime_env["db_path"]).resolve()

    assert project_root not in runtime_root.parents
    assert runtime_root in db_path.parents
    assert db_path.parts[-4:] == ("data", "paper", "trades", "paper.sqlite")


def test_oms_smoke_rejects_repo_local_db_path() -> None:
    repo_local_db = (Path.cwd() / "tmp" / "paper" / "repo-local-smoke.sqlite").resolve()
    env = dict(os.environ)
    env["PYTHONPATH"] = "src"
    env["DB_PATH"] = str(repo_local_db)

    proc = _run_oms_smoke(env=env)

    assert proc.returncode != 0
    assert "Refusing repo-local DB path for smoke/manual run" in (proc.stdout + proc.stderr)


def test_oms_smoke_allows_explicit_external_tmp_db_path(tmp_path: Path) -> None:
    external_db = (tmp_path / "runtime" / "data" / "paper" / "trades" / "paper.sqlite").resolve()
    external_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(external_db)
    try:
        conn.execute("CREATE TABLE candles (pair TEXT, interval TEXT, ts INTEGER, close REAL)")
        conn.commit()
    finally:
        conn.close()

    env = dict(os.environ)
    env["PYTHONPATH"] = "src"
    env["DB_PATH"] = str(external_db)

    proc = _run_oms_smoke(env=env)

    # Path policy checks passed; command fails later because smoke precondition(candles) is missing.
    assert proc.returncode != 0
    assert "No candles." in (proc.stdout + proc.stderr)
    assert "Refusing repo-local DB path" not in (proc.stdout + proc.stderr)
