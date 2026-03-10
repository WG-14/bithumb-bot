from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from bithumb_bot.run_lock import (
    STALE_LOCK_MAX_AGE_SECONDS,
    RunLockError,
    acquire_run_lock,
    read_run_lock_status,
)


def test_second_acquire_fails_while_first_is_held(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"

    with acquire_run_lock(lock_path):
        with pytest.raises(RunLockError) as exc:
            with acquire_run_lock(lock_path):
                pass

    message = str(exc.value)
    assert "already running" in message
    assert "owner_pid=" in message


def test_lock_can_be_reacquired_after_release(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"

    with acquire_run_lock(lock_path):
        pass

    with acquire_run_lock(lock_path):
        pass


def test_stale_lock_is_reclaimed_when_not_actively_held(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    lock_path = tmp_path / "run.lock"
    lock_path.write_text("999999\n", encoding="utf-8")
    stale_mtime = time.time() - (STALE_LOCK_MAX_AGE_SECONDS + 5)
    os.utime(lock_path, (stale_mtime, stale_mtime))

    with acquire_run_lock(lock_path):
        owner_text = lock_path.read_text(encoding="utf-8").strip()
        assert f"pid={os.getpid()}" in owner_text
        assert "host=" in owner_text
        assert "created_at=" in owner_text

    assert "reclaiming stale run lock file" in caplog.text


def test_stale_lock_with_non_pid_owner_text_is_reclaimed(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    lock_path = tmp_path / "run.lock"
    lock_path.write_text("legacy-owner\n", encoding="utf-8")
    stale_mtime = time.time() - (STALE_LOCK_MAX_AGE_SECONDS + 5)
    os.utime(lock_path, (stale_mtime, stale_mtime))

    with acquire_run_lock(lock_path):
        owner_text = lock_path.read_text(encoding="utf-8").strip()
        assert f"pid={os.getpid()}" in owner_text
        assert "host=" in owner_text
        assert "created_at=" in owner_text

    assert "reclaiming stale run lock file" in caplog.text

def test_error_message_includes_lock_owner_context_on_collision(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"

    with acquire_run_lock(lock_path):
        with pytest.raises(RunLockError) as exc:
            with acquire_run_lock(lock_path):
                pass

    message = str(exc.value)
    assert f"lock: {lock_path}" in message
    assert "owner_pid=" in message
    assert "owner_host=" in message
    assert "owner_created_at=" in message
    assert "lock_age=" in message


def test_abnormal_termination_stale_lock_file_is_reclaimed(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"

    child_code = textwrap.dedent(
        """
        import os
        import signal
        import sys
        from pathlib import Path

        from bithumb_bot.run_lock import acquire_run_lock

        lock_path = Path(sys.argv[1])
        with acquire_run_lock(lock_path):
            print("LOCKED", flush=True)
            os.kill(os.getpid(), signal.SIGKILL)
        """
    )

    proc = subprocess.Popen(
        [sys.executable, "-c", child_code, str(lock_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONPATH": str(Path.cwd() / "src")},
    )
    stdout, _stderr = proc.communicate(timeout=10)

    assert "LOCKED" in stdout
    assert proc.returncode not in (0, None)

    stale_mtime = time.time() - (STALE_LOCK_MAX_AGE_SECONDS + 5)
    os.utime(lock_path, (stale_mtime, stale_mtime))

    with acquire_run_lock(lock_path):
        owner_text = lock_path.read_text(encoding="utf-8").strip()
        assert f"pid={os.getpid()}" in owner_text


def test_read_run_lock_status_reports_live_owner(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"

    with acquire_run_lock(lock_path):
        status = read_run_lock_status(lock_path)

    assert status.lock_path == lock_path
    assert status.owner_pid == os.getpid()
    assert status.owner_hostname is not None
    assert status.created_at is not None
    assert status.age_seconds is not None
    assert status.is_stale_candidate is False
    assert "live owner" in status.to_human_text()
    assert "host=" in status.to_human_text()
    assert "created_at=" in status.to_human_text()


def test_read_run_lock_status_reports_stale_candidate(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"
    lock_path.write_text("999999\n", encoding="utf-8")
    stale_mtime = time.time() - (STALE_LOCK_MAX_AGE_SECONDS + 5)
    os.utime(lock_path, (stale_mtime, stale_mtime))

    status = read_run_lock_status(lock_path)

    assert status.lock_path == lock_path
    assert status.owner_pid == 999999
    assert status.owner_hostname is None
    assert status.created_at is None
    assert status.age_seconds is not None
    assert status.is_stale_candidate is True
    assert "stale candidate" in status.to_human_text()


def test_read_run_lock_status_parses_rich_owner_record(tmp_path: Path) -> None:
    lock_path = tmp_path / "run.lock"
    lock_path.write_text(
        "pid=1234 host=test-host created_at=2026-01-01T00:00:00+00:00\n",
        encoding="utf-8",
    )

    status = read_run_lock_status(lock_path)

    assert status.owner_pid == 1234
    assert status.owner_hostname == "test-host"
    assert status.created_at == "2026-01-01T00:00:00+00:00"
