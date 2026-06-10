from __future__ import annotations

import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fake_notify(tmp_path: Path, *, exit_code: int) -> Path:
    helper = tmp_path / "fake_notify.sh"
    helper.write_text(
        "#!/usr/bin/env bash\n"
        "printf 'fake helper called\\n' >&2\n"
        f"exit {exit_code}\n",
        encoding="utf-8",
    )
    helper.chmod(0o755)
    return helper


def _run_notify_function(script: str, tmp_path: Path, *, exit_code: int) -> subprocess.CompletedProcess[str]:
    helper = _fake_notify(tmp_path, exit_code=exit_code)
    command = (
        f"source <(sed -n '/^notify() {{/,/^}}/p' {script}); "
        "stage=test-stage; "
        f"NOTIFY_SCRIPT={helper}; "
        "NTFY_TOPIC=topic-secret; "
        f"CODEX_PYTEST_WORK_DIR={tmp_path}; "
        "notify Title default Message"
    )
    return subprocess.run(
        ["bash", "-c", command],
        cwd=ROOT,
        env=os.environ.copy(),
        text=True,
        capture_output=True,
        check=False,
    )


def test_codex_pipeline_notify_records_helper_failure(tmp_path: Path) -> None:
    result = _run_notify_function("scripts/run_codex_pipeline.sh", tmp_path, exit_code=22)

    assert result.returncode == 0
    assert "[NOTIFY-RESULT] transport=ntfy status=failed exit_code=22 stage=test-stage" in result.stderr


def test_codex_pytest_pipeline_notify_records_helper_failure(tmp_path: Path) -> None:
    result = _run_notify_function("scripts/run_codex_pytest_pipeline.sh", tmp_path, exit_code=22)

    assert result.returncode == 0
    assert "[NOTIFY-RESULT] transport=ntfy status=failed exit_code=22 stage=test-stage" in result.stderr
    artifact = tmp_path / "notification_result.jsonl"
    assert artifact.exists()
    assert '"status":"failed"' in artifact.read_text(encoding="utf-8")


def test_notify_result_does_not_include_topic_secret(tmp_path: Path) -> None:
    result = _run_notify_function("scripts/run_codex_pytest_pipeline.sh", tmp_path, exit_code=22)
    artifact = tmp_path / "notification_result.jsonl"
    combined = result.stderr + artifact.read_text(encoding="utf-8")

    assert "topic-secret" not in combined
