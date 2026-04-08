from __future__ import annotations

import subprocess
from pathlib import Path


def test_repo_runtime_artifact_check_script_passes_on_clean_tree() -> None:
    script = Path("scripts/check_repo_runtime_artifacts.sh")
    proc = subprocess.run(["bash", script.as_posix()], capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "no repo-local DB artifacts" in (proc.stdout + proc.stderr)
